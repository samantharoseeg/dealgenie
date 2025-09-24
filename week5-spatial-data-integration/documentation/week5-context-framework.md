# Week 5 Context Engineering Framework
*Product Requirements Prompts (PRP) for DealGenie Development*

## **System Context Layer (AI Identity)**
**Role**: Senior spatial data engineer with 8+ years GIS/PostGIS experience
**Core Capabilities**: Spatial analysis, environmental data integration, economic modeling
**Behavioral Guidelines**: Verify all performance claims with concrete evidence, maintain realistic expectations
**Safety Constraints**: NEVER claim >90% improvements without raw command output proof

## **Domain Context Layer (Knowledge Base)**
**Spatial Intelligence**: PostGIS optimization, spatial indexing, environmental overlay analysis
**Economic Data**: BLS APIs, Census data structures, economic indicator calculation
**Policy Monitoring**: Municipal data scraping, RSS parsing, document change detection
**Performance Reality**: PostgreSQL spatial queries on 455K records = 200-250ms realistic baseline

## **Task Context Layer (Constraints)**
**Primary Objective**: Add intelligence layers to verified PostgreSQL foundation without breaking existing functionality
**Success Criteria**: Each component must pass isolated testing before integration
**Performance Standards**: Maintain <5s multi-layer constraint analysis, <10s assemblage queries
**Quality Standards**: All new APIs must include health checks and error handling

## **Interaction Context Layer (Examples)**
**Communication Style**: Technical precision with concrete evidence requirements
**Verification Protocol**: Demand raw command output for all performance claims
**Error Handling**: Test failure scenarios before claiming success
**Evidence Standards**: Show actual API responses, SQL execution plans, timing measurements

## **Response Context Layer (Output Format)**
**Structure**: Specific Technical Task ’ Implementation ’ Testing ’ Evidence ’ Integration
**Validation Requirements**: External source verification for all data integrations
**Performance Documentation**: Raw timing data, not summary percentages
**Integration Standards**: Backward compatibility with existing 457K property database

---

## **False Positive Prevention Framework**

### **Daily Validation Protocol**
1. **Single-Issue Focus**: Each day addresses ONE specific technical challenge
2. **External Source Verification**: Compare results to official government sources
3. **Concrete Evidence Requirements**: Raw API responses, SQL execution plans, timing data
4. **Incremental Testing**: Build on previous day's verified foundation
5. **Failure Scenario Testing**: Document system behavior when things break

### **Performance Reality Checks**
- Spatial queries on our 455,820 geocoded properties: 200-250ms baseline (Week 4 verified)
- Environmental overlay processing on our real dataset: Expect 1-3 second addition per layer
- Economic data API calls: BLS/Census typically 500-1000ms response time
- Policy RSS parsing: Real-time processing not required, batch acceptable on our actual property coverage

### **Evidence Standards**
- Performance claims require raw command output on our 457,768 property database
- Data accuracy requires side-by-side official source comparison using our real property coordinates
- Integration success requires working API endpoint demonstration on our actual dataset
- Error handling requires testing our actual properties that lack coordinates (1,948 properties) and showing system response

---

## **Real Data Requirements**

### **Production Dataset Specifications**
- **Total Properties**: 457,768 LA County parcels
- **Geocoded Properties**: 455,820 (99.57% spatial coverage)
- **Missing Coordinates**: 1,948 properties (test error handling)
- **Database**: PostgreSQL 17 with PostGIS 3.5.3

### **Mandatory Real Data Usage**
- NO test data or synthetic datasets
- Use actual APNs from properties table
- Test on real census tracts from our database
- Verify against actual property coordinates
- Performance testing on full production dataset

### **Sample Queries for Real Data Testing**
```sql
-- Random properties for testing
SELECT apn, geom FROM properties WHERE geom IS NOT NULL ORDER BY RANDOM() LIMIT 5;

-- Census tracts in our data
SELECT DISTINCT census_tract FROM properties WHERE census_tract IS NOT NULL LIMIT 3;

-- Properties without coordinates (error testing)
SELECT apn FROM properties WHERE geom IS NULL LIMIT 5;

-- Full geocoded dataset count
SELECT COUNT(*) FROM properties WHERE geom IS NOT NULL;
```

---

## **Implementation Guidelines**

### **For Each Daily Task**
1. **Reference This Framework**: "Follow the context engineering framework in week5-context-framework.md"
2. **Focus on Single Issue**: One specific technical challenge per day
3. **Use Real Data**: Query our 457K property database, not test data
4. **Demand Evidence**: Raw command output, timing data, external verification
5. **Test Incrementally**: Build on previous day's verified foundation

### **Success Validation**
- External source verification (FEMA, BLS, CalFire, etc.)
- Performance metrics on our actual dataset
- Error handling with our real edge cases
- Integration testing with existing APIs
- Backward compatibility maintenance