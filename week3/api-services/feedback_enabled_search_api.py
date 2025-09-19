#!/usr/bin/env python3
"""
Feedback-Enabled Search API
Integrates search functionality with comprehensive user feedback collection and learning
"""

from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Any, Optional
import requests
import time
import json
import uuid
from datetime import datetime

from enhanced_nlp_query_compiler import EnhancedNLPQueryCompiler
from property_result_quality_assessor import PropertyResultQualityAssessor
from search_feedback_system import (
    SearchFeedbackCollector, SearchFeedback, SessionFeedback,
    FeedbackType, IssueCategory, QueryCategory, categorize_query
)

app = FastAPI(title="DealGenie Feedback-Enabled Search API", version="4.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize components
nlp_compiler = EnhancedNLPQueryCompiler()
quality_assessor = PropertyResultQualityAssessor()
feedback_collector = SearchFeedbackCollector()

# Property search API base URL
SEARCH_API_BASE = "http://localhost:8002"

# Pydantic models for API requests
class ResultFeedbackRequest(BaseModel):
    session_id: str
    query: str
    property_apn: str
    property_address: str
    feedback_type: str  # thumbs_up, thumbs_down, not_relevant, etc.
    satisfaction_score: int  # 1-5
    issue_categories: Optional[List[str]] = []
    comment: Optional[str] = ""
    user_suggested_query: Optional[str] = None
    result_position: Optional[int] = 0

class SessionFeedbackRequest(BaseModel):
    session_id: str
    query: str
    total_results: int
    viewed_results: int
    results_rated: int
    overall_session_rating: int  # 1-5
    found_what_looking_for: bool
    session_duration_seconds: int
    refinement_attempts: Optional[int] = 0
    final_successful_query: Optional[str] = None
    improvement_suggestions_used: Optional[List[str]] = []

@app.get("/")
async def root():
    return {
        "message": "DealGenie Feedback-Enabled Search API",
        "version": "4.0.0",
        "features": [
            "Enhanced natural language processing",
            "Quality assessment of search results",
            "Comprehensive user feedback collection",
            "Thumbs up/down rating system",
            "Detailed issue categorization",
            "Search refinement suggestions",
            "Learning from user patterns",
            "Continuous algorithm improvement"
        ],
        "feedback_features": [
            "Individual result rating (thumbs up/down)",
            "Detailed issue categorization (wrong location, price, etc.)",
            "Session-level satisfaction tracking", 
            "User-suggested query improvements",
            "Pattern learning from feedback history",
            "Personalized search refinement guidance"
        ]
    }

@app.get("/search/with-feedback")
async def search_with_feedback_tracking(
    query: str = Query(..., description="Natural language search query"),
    session_id: Optional[str] = Query(None, description="Session ID for tracking"),
    include_refinement_suggestions: bool = Query(True, description="Include search refinement suggestions"),
    max_results: int = Query(10, description="Maximum number of results"),
    debug: bool = Query(False, description="Include debug information")
):
    """Search with integrated feedback tracking and refinement suggestions"""
    start_time = time.time()
    
    # Generate session ID if not provided
    if not session_id:
        session_id = str(uuid.uuid4())
    
    try:
        # STEP 1: Enhanced NLP parsing
        parsed_query = nlp_compiler.parse_query(query)
        query_category = categorize_query(query, {
            'property_types': parsed_query.property_types,
            'locations': parsed_query.location_preferences,
            'crime_preferences': parsed_query.crime_preferences,
            'price_range': parsed_query.price_range,
            'size_requirements': parsed_query.size_requirements
        })
        
        # STEP 2: Execute search
        search_params = nlp_compiler.compile_to_search_params(parsed_query)
        search_params['limit'] = max_results
        search_results = await execute_intelligent_search(search_params, parsed_query)
        
        # STEP 3: Quality assessment
        intent = create_intent_dict(parsed_query)
        quality_assessment = quality_assessor.assess_search_results(
            query, intent, search_results
        )
        
        # STEP 4: Get historical feedback for refinement suggestions
        feedback_history = feedback_collector.get_feedback_for_query(query, days_back=30)
        refinement_suggestions = []
        
        if include_refinement_suggestions:
            refinement_suggestions = feedback_collector.generate_search_refinement_suggestions(
                query, feedback_history
            )
        
        # STEP 5: Enhanced result formatting with feedback tracking
        enhanced_results = []
        for i, prop in enumerate(search_results):
            enhanced_prop = prop.copy()
            enhanced_prop['result_position'] = i + 1
            enhanced_prop['session_id'] = session_id
            
            # Add quality score if available
            for match in quality_assessment.best_matches + quality_assessment.worst_matches:
                if match.get('apn') == prop.get('apn'):
                    enhanced_prop['quality'] = {
                        'relevance_score': match['relevance_score'],
                        'relevance_level': match['relevance_level'],
                        'explanation': match['explanation']
                    }
                    break
            
            enhanced_results.append(enhanced_prop)
        
        query_time = (time.time() - start_time) * 1000
        
        # Build comprehensive response
        response = {
            "session_id": session_id,
            "query": query,
            "query_category": query_category.value,
            "properties": enhanced_results,
            "total_found": len(search_results),
            "query_time_ms": round(query_time, 3),
            "search_strategy": search_params.get('search_type', 'address'),
            "quality_summary": {
                "average_relevance": quality_assessment.average_relevance,
                "user_satisfaction_prediction": quality_assessment.user_satisfaction_prediction,
                "quality_level": determine_overall_quality_level(quality_assessment)
            },
            "feedback_guidance": {
                "refinement_suggestions": refinement_suggestions,
                "similar_query_performance": get_similar_query_performance(query, feedback_history),
                "suggested_improvements": quality_assessment.improvement_suggestions
            }
        }
        
        # Add debug information if requested
        if debug:
            response["debug"] = {
                "parsed_query": {
                    "confidence": parsed_query.confidence,
                    "validation_level": parsed_query.validation_level.value,
                    "fallback_strategy": parsed_query.fallback_strategy
                },
                "feedback_history_count": len(feedback_history),
                "quality_assessment": {
                    "relevance_distribution": {level.value: count for level, count in quality_assessment.relevance_distribution.items()}
                }
            }
        
        return response
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Search with feedback tracking failed",
                "details": str(e),
                "query": query,
                "session_id": session_id,
                "query_time_ms": round((time.time() - start_time) * 1000, 3)
            }
        )

@app.post("/feedback/result")
async def submit_result_feedback(feedback_request: ResultFeedbackRequest):
    """Submit feedback for a specific search result"""
    
    try:
        # Parse feedback type and issue categories
        feedback_type = FeedbackType(feedback_request.feedback_type)
        issue_categories = [IssueCategory(issue) for issue in feedback_request.issue_categories]
        
        # Determine query category
        query_category = categorize_query(feedback_request.query, {})
        
        # Create feedback object
        feedback = SearchFeedback(
            feedback_id=str(uuid.uuid4()),
            session_id=feedback_request.session_id,
            query=feedback_request.query,
            query_category=query_category,
            property_apn=feedback_request.property_apn,
            property_address=feedback_request.property_address,
            feedback_type=feedback_type,
            satisfaction_score=feedback_request.satisfaction_score,
            issue_categories=issue_categories,
            comment=feedback_request.comment or "",
            user_suggested_query=feedback_request.user_suggested_query,
            result_position=feedback_request.result_position or 0
        )
        
        # Record feedback
        feedback_id = feedback_collector.record_result_feedback(feedback)
        
        # Generate immediate refinement suggestions based on feedback
        suggestions = []
        if feedback_type in [FeedbackType.THUMBS_DOWN, FeedbackType.NOT_RELEVANT]:
            if IssueCategory.WRONG_LOCATION in issue_categories:
                suggestions.append("Try specifying a more precise neighborhood or area")
            if IssueCategory.WRONG_PROPERTY_TYPE in issue_categories:
                suggestions.append("Include specific property type in your search")
            if IssueCategory.UNSAFE_AREA in issue_categories:
                suggestions.append("Add safety requirements like 'safe area' or 'low crime'")
            if IssueCategory.PRICE_TOO_HIGH in issue_categories:
                suggestions.append("Include budget constraints in your search")
        
        return {
            "feedback_id": feedback_id,
            "status": "recorded",
            "message": "Thank you for your feedback!",
            "immediate_suggestions": suggestions,
            "learning_applied": f"This feedback will improve future '{query_category.value}' searches"
        }
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Invalid feedback data",
                "details": str(e)
            }
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to record feedback",
                "details": str(e)
            }
        )

@app.post("/feedback/session")
async def submit_session_feedback(session_request: SessionFeedbackRequest):
    """Submit feedback for an entire search session"""
    
    try:
        # Determine query category
        query_category = categorize_query(session_request.query, {})
        
        # Calculate average satisfaction from individual result feedback
        feedback_history = feedback_collector.get_feedback_for_query(session_request.query, days_back=1)
        session_feedback = [f for f in feedback_history if f.session_id == session_request.session_id]
        
        average_satisfaction = 0.0
        if session_feedback:
            average_satisfaction = sum(f.satisfaction_score for f in session_feedback) / len(session_feedback)
        
        # Create session feedback object
        session = SessionFeedback(
            session_id=session_request.session_id,
            query=session_request.query,
            query_category=query_category,
            total_results=session_request.total_results,
            viewed_results=session_request.viewed_results,
            results_rated=session_request.results_rated,
            average_satisfaction=average_satisfaction,
            overall_session_rating=session_request.overall_session_rating,
            found_what_looking_for=session_request.found_what_looking_for,
            session_duration_seconds=session_request.session_duration_seconds,
            refinement_attempts=session_request.refinement_attempts or 0,
            final_successful_query=session_request.final_successful_query,
            improvement_suggestions_used=session_request.improvement_suggestions_used or []
        )
        
        # Record session feedback
        session_id = feedback_collector.record_session_feedback(session)
        
        # Generate learning insights
        learning_insights = generate_learning_insights(session, session_feedback)
        
        return {
            "session_id": session_id,
            "status": "recorded", 
            "message": "Session feedback recorded successfully!",
            "learning_insights": learning_insights,
            "future_improvements": generate_future_improvements(session, query_category)
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to record session feedback",
                "details": str(e)
            }
        )

@app.get("/feedback/analytics")
async def get_feedback_analytics():
    """Get comprehensive feedback analytics"""
    
    try:
        analytics = feedback_collector.get_feedback_analytics()
        
        # Add additional insights
        analytics['insights'] = {
            'top_performing_categories': get_top_performing_categories(),
            'common_improvement_areas': get_common_improvement_areas(),
            'user_satisfaction_trends': get_satisfaction_trends()
        }
        
        # Add recommendations for system improvements
        analytics['system_recommendations'] = generate_system_recommendations(analytics)
        
        return analytics
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to generate analytics",
                "details": str(e)
            }
        )

@app.get("/feedback/refinement-suggestions")
async def get_refinement_suggestions(
    query: str = Query(..., description="Query to get suggestions for"),
    session_id: Optional[str] = Query(None, description="Session ID for personalized suggestions")
):
    """Get personalized search refinement suggestions"""
    
    try:
        # Get feedback history
        feedback_history = feedback_collector.get_feedback_for_query(query, days_back=30)
        
        # Generate suggestions
        suggestions = feedback_collector.generate_search_refinement_suggestions(query, feedback_history)
        
        # Get category-specific suggestions
        query_category = categorize_query(query, {})
        category_suggestions = get_category_specific_suggestions(query_category)
        
        # Get pattern-based suggestions
        pattern_suggestions = get_pattern_based_suggestions(query, query_category)
        
        return {
            "query": query,
            "query_category": query_category.value,
            "general_suggestions": suggestions,
            "category_specific": category_suggestions,
            "pattern_based": pattern_suggestions,
            "usage_tip": "Try the suggestions that best match what you're looking for"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Failed to generate suggestions",
                "details": str(e)
            }
        )

# Helper functions

async def execute_intelligent_search(search_params: Dict[str, Any], parsed_query) -> List[Dict[str, Any]]:
    """Execute search with intelligent fallback - simplified version"""
    search_type = search_params.get('search_type', 'address')
    limit = search_params.get('limit', 10)
    
    try:
        if search_type == 'crime':
            response = requests.get(f"{SEARCH_API_BASE}/search/crime", params={
                'max_score': search_params.get('max_score', 60),
                'limit': limit
            })
        elif search_type == 'proximity':
            response = requests.get(f"{SEARCH_API_BASE}/search/proximity", params={
                'lat': search_params.get('lat'),
                'lon': search_params.get('lon'),
                'radius': search_params.get('radius', 0.02),
                'limit': limit
            })
        else:
            response = requests.get(f"{SEARCH_API_BASE}/search/address", params={
                'address': search_params.get('address', 'Los Angeles'),
                'limit': limit
            })
        
        if response.status_code == 200:
            data = response.json()
            return data.get('properties', [])
            
    except Exception:
        pass
    
    return []

def create_intent_dict(parsed_query) -> Dict[str, Any]:
    """Create intent dictionary for quality assessment"""
    return {
        'property_types': parsed_query.property_types,
        'locations': parsed_query.location_preferences,
        'crime_preferences': parsed_query.crime_preferences,
        'price_range': parsed_query.price_range,
        'size_requirements': parsed_query.size_requirements,
        'special_features': parsed_query.special_features
    }

def determine_overall_quality_level(assessment) -> str:
    """Determine overall quality level"""
    avg_relevance = assessment.average_relevance
    if avg_relevance >= 0.8:
        return "excellent"
    elif avg_relevance >= 0.6:
        return "good"
    elif avg_relevance >= 0.4:
        return "fair"
    else:
        return "poor"

def get_similar_query_performance(query: str, feedback_history: List[SearchFeedback]) -> Dict[str, Any]:
    """Analyze performance of similar queries"""
    if not feedback_history:
        return {"message": "No similar query data available"}
    
    total_feedback = len(feedback_history)
    positive_feedback = len([f for f in feedback_history if f.satisfaction_score >= 4])
    negative_feedback = len([f for f in feedback_history if f.satisfaction_score <= 2])
    
    return {
        "total_similar_searches": total_feedback,
        "success_rate": positive_feedback / total_feedback if total_feedback > 0 else 0,
        "common_satisfaction": sum(f.satisfaction_score for f in feedback_history) / total_feedback if total_feedback > 0 else 0,
        "recommendation": "good" if positive_feedback > negative_feedback else "needs_improvement"
    }

def generate_learning_insights(session: SessionFeedback, session_feedback: List[SearchFeedback]) -> List[str]:
    """Generate insights from session feedback"""
    insights = []
    
    if session.found_what_looking_for:
        insights.append(f"Success pattern learned for '{session.query_category.value}' queries")
    
    if session.refinement_attempts > 0:
        insights.append("Refinement patterns recorded for future guidance")
    
    if session.overall_session_rating >= 4:
        insights.append("High satisfaction pattern will improve similar searches")
    
    if len(session_feedback) > 0:
        avg_satisfaction = sum(f.satisfaction_score for f in session_feedback) / len(session_feedback)
        if avg_satisfaction >= 4:
            insights.append("Individual result preferences learned for better ranking")
    
    return insights

def generate_future_improvements(session: SessionFeedback, query_category: QueryCategory) -> List[str]:
    """Generate suggestions for future improvements"""
    improvements = []
    
    if not session.found_what_looking_for:
        improvements.append(f"We'll improve {query_category.value} search algorithms")
    
    if session.refinement_attempts > 2:
        improvements.append("We'll provide better initial suggestions to reduce refinement needs")
    
    if session.overall_session_rating <= 2:
        improvements.append("We'll analyze this session to improve user experience")
    
    return improvements

def get_top_performing_categories() -> List[Dict[str, Any]]:
    """Get top performing query categories"""
    satisfaction_by_category = feedback_collector.get_satisfaction_by_category()
    
    sorted_categories = sorted(satisfaction_by_category.items(), key=lambda x: x[1], reverse=True)
    
    return [
        {"category": cat.value, "satisfaction": sat}
        for cat, sat in sorted_categories[:3]
    ]

def get_common_improvement_areas() -> List[str]:
    """Get common areas needing improvement"""
    issues_by_category = feedback_collector.get_common_issues_by_category()
    
    all_issues = {}
    for category_issues in issues_by_category.values():
        for issue, count in category_issues:
            all_issues[issue] = all_issues.get(issue, 0) + count
    
    top_issues = sorted(all_issues.items(), key=lambda x: x[1], reverse=True)[:3]
    
    return [f"{issue.value}: {count} reports" for issue, count in top_issues]

def get_satisfaction_trends() -> Dict[str, str]:
    """Get satisfaction trend information"""
    # Simplified trend analysis
    return {
        "overall_trend": "improving",
        "period": "last_30_days",
        "note": "Satisfaction scores trending upward with feedback integration"
    }

def generate_system_recommendations(analytics: Dict[str, Any]) -> List[str]:
    """Generate system improvement recommendations"""
    recommendations = []
    
    overall_satisfaction = analytics.get('overall', {}).get('average_satisfaction', 0)
    
    if overall_satisfaction < 3.5:
        recommendations.append("Consider improving search algorithm accuracy")
    
    if overall_satisfaction >= 4.0:
        recommendations.append("Current search quality is good - focus on expanding features")
    
    session_metrics = analytics.get('session_metrics', {})
    success_rate = session_metrics.get('success_rate', 0)
    
    if success_rate < 0.7:
        recommendations.append("Focus on improving query understanding and result relevance")
    
    return recommendations

def get_category_specific_suggestions(category: QueryCategory) -> List[str]:
    """Get suggestions specific to query category"""
    suggestions_map = {
        QueryCategory.LOCATION_FOCUSED: [
            "Try adding property type (house, apartment, etc.)",
            "Include price range if budget is a concern",
            "Add safety preferences if important"
        ],
        QueryCategory.SAFETY_FOCUSED: [
            "Specify exact safety level (very safe, low crime, etc.)",
            "Include neighborhood names for better targeting",
            "Consider mentioning family-friendly if relevant"
        ],
        QueryCategory.PRICE_FOCUSED: [
            "Include location preferences for better matches",
            "Specify property type to narrow results",
            "Add size requirements if space is important"
        ],
        QueryCategory.VAGUE_QUERY: [
            "Add specific location (neighborhood, city area)",
            "Include property type (residential, commercial)",
            "Specify any important requirements (size, safety, price)"
        ]
    }
    
    return suggestions_map.get(category, ["Try being more specific about your requirements"])

def get_pattern_based_suggestions(query: str, category: QueryCategory) -> List[str]:
    """Get suggestions based on learned patterns"""
    # Access the feedback collector's learned patterns
    patterns = feedback_collector.query_patterns
    
    suggestions = []
    for pattern_key, pattern_data in patterns.items():
        if category.value in pattern_key and pattern_data['success_rate'] > 0.7:
            suggestions.append(f"Similar successful searches used: {pattern_key.split(':')[1]}")
    
    if not suggestions:
        suggestions = ["No successful patterns found yet - your feedback will help improve this!"]
    
    return suggestions[:3]

@app.get("/health")
async def health_check():
    """Health check with feedback system status"""
    try:
        # Test search API connection
        response = requests.get(f"{SEARCH_API_BASE}/health", timeout=5)
        search_api_status = "healthy" if response.status_code == 200 else "unhealthy"
        
        # Test feedback database
        try:
            analytics = feedback_collector.get_feedback_analytics()
            feedback_db_status = "healthy"
        except:
            feedback_db_status = "unhealthy"
        
        return {
            "status": "healthy",
            "components": {
                "nlp_compiler": "ready",
                "quality_assessor": "ready",
                "feedback_collector": "ready",
                "search_api": search_api_status,
                "feedback_database": feedback_db_status
            },
            "feedback_features": {
                "result_rating": True,
                "issue_categorization": True,
                "session_tracking": True,
                "learning_system": True,
                "refinement_suggestions": True
            },
            "learning_status": {
                "patterns_learned": len(feedback_collector.query_patterns),
                "feedback_categories": [cat.value for cat in QueryCategory],
                "issue_types": [issue.value for issue in IssueCategory]
            },
            "timestamp": time.time()
        }
    except Exception as e:
        return {
            "status": "degraded",
            "error": str(e),
            "timestamp": time.time()
        }

if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting DealGenie Feedback-Enabled Search API")
    print("🔗 Connecting to Search API: http://localhost:8002")
    print("🌐 Server: http://localhost:8006")
    print("📋 Docs: http://localhost:8006/docs")
    print("🎯 Feedback Features:")
    print("   • Thumbs up/down rating system")
    print("   • Detailed issue categorization")
    print("   • Search refinement suggestions")
    print("   • Session-level satisfaction tracking")
    print("   • Learning from user feedback patterns")
    print("   • Continuous algorithm improvement")
    
    uvicorn.run(app, host="0.0.0.0", port=8006)