DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM EdFiDW.[dbo].[Lineage] WHERE TableName= 'dbo.DimAssessment')
BEGIN
    INSERT INTO EdFiDW.[dbo].[Lineage]
	(
	 [TableName], 
	 [StartTime], 
	 [EndTime], 
	 [LoadType], 
	 [Status]
	)
	VALUES
	('dbo.DimAssessment', 
	 GETDATE(), 
	 NULL, 
	 'F'
	 , -- full 
	 ''
	); -- Processing
	SET @lineageKey = SCOPE_IDENTITY() ;
END;
ELSE
BEGIN
     SELECT @lineageKey = LineageKey
	 FROM EdFiDW.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.DimAssessment'
END 

DECLARE @Assessment TABLE
(   
	AssessmentCategoryDescriptor_CodeValue NVARCHAR(50) NOT NULL,    
	AssessmentCategoryDescriptor_Description NVARCHAR(1024) NOT NULL,    
	AssessmentFamilyTitle NVARCHAR(100) NULL,    	
	AdaptiveAssessment_Indicator bit NOT NULL, 
	AssessmentIdentifier NVARCHAR(60) NOT NULL,   
	ObjectiveAssessmentIdentificationCode NVARCHAR(60) NOT NULL,   
	AssessmentTitle NVARCHAR(500) NOT NULL,

	ReportingMethodDescriptor_CodeValue NVARCHAR(50) NOT NULL,   
	ReportingMethodDescriptor_Description NVARCHAR(1024) NOT NULL,   
	
	ResultDatatypeTypeDescriptor_CodeValue  NVARCHAR(50) NOT NULL,   
	ResultDatatypeTypeDescriptor_Description NVARCHAR(1024) NOT NULL,   


	AssessmentScore_Indicator  BIT NOT NULL,
	AssessmentPerformanceLevel_Indicator  BIT NOT NULL,

	ObjectiveAssessmentScore_Indicator  BIT NOT NULL,
	ObjectiveAssessmentPerformanceLevel_Indicator  BIT NOT NULL

);



--Assessmnent
;WITH UnpivotedScores AS 
(
	SELECT testid,schyear,testtime,studentno,adminyear,grade, scoretype, scorevalue, CASE WHEN a.scoretype IN ('Proficiency level','Proficiency level 2') THEN 1 ELSE 0 END AS isperflevel
	--INTO [Raw_LegacyDW].[MCASAssessmentScores]
	FROM (  
			 --ensuring all score columns have the same data type to avoid conflicts with unpivot
			 SELECT testid,schyear,testtime,studentno,adminyear,grade,teststatus ,
				  CAST(rawscore AS NVARCHAR(MAX)) AS [Raw score],
				  CAST(scaledscore AS NVARCHAR(MAX)) AS [Scale score],
				  CAST(perflevel AS NVARCHAR(MAX)) AS  [Proficiency level],
				  CAST(sgp AS NVARCHAR(MAX)) AS [Percentile rank],
				  CAST(cpi AS NVARCHAR(MAX)) AS [Composite Performance Index],
				  CAST(perf2 AS NVARCHAR(MAX)) AS [Proficiency level 2]
     
			FROM [BPSGranary02].[RAEDatabase].[dbo].[mcasitems] 
			WHERE schyear >= 2015 ) scores
	UNPIVOT
	(
	   scorevalue
	   FOR scoretype IN ([Raw score],[Scale score],[Proficiency level],[Percentile rank],[Composite Performance Index],[Proficiency level 2])
	) AS a

--Composite Rating
--Composite Score ?
--Percentile rank
--Promotion score ?
--Raw score
--Scale score
--Proficiency level

)
INSERT INTO @Assessment
(
    AssessmentCategoryDescriptor_CodeValue,
    AssessmentCategoryDescriptor_Description,
    AssessmentFamilyTitle,
    AdaptiveAssessment_Indicator,
    AssessmentIdentifier,
	ObjectiveAssessmentIdentificationCode,
    AssessmentTitle,

    ReportingMethodDescriptor_CodeValue,   
	ReportingMethodDescriptor_Description,   
	
	ResultDatatypeTypeDescriptor_CodeValue,   
	ResultDatatypeTypeDescriptor_Description,   


	AssessmentScore_Indicator,
	AssessmentPerformanceLevel_Indicator,

	ObjectiveAssessmentScore_Indicator,
	ObjectiveAssessmentPerformanceLevel_Indicator
)

SELECT DISTINCT 
	   'State assessment' AS [AssessmentCategoryDescriptor_CodeValue],
	   'State assessment' AS [AssessmentCategoryDescriptor_Description],
	   NULL AS [AssessmentFamilyTitle],
	   0 AS [AdaptiveAssessment_Indicator], 
	   testid AS AssessmentIdentifier,
	   'N/A' AS ObjectiveAssessmentIdentificationCode,
	   testid AS AssessmentTitle,	   

       scoretype AS ReportingMethodDescriptor_CodeValue,
	   scoretype AS ReportingMethodDescriptor_Description,
	   CASE WHEN isperflevel = 1 THEN 'Level' ELSE 'Integer' end   AS ResultDatatypeTypeDescriptor_CodeValue,
	   CASE WHEN isperflevel = 1 THEN 'Level' ELSE 'Integer' end AS ResultDatatypeTypeDescriptor_Description,
	   
	   CASE WHEN isperflevel = 1 THEN 0 ELSE 1 end AS AssessmentScore_Indicator,
	   isperflevel AS AssessmentPerformanceLevel_Indicator,
  
	   0 AS ObjectiveAssessmentScore_Indicator,
	   0 AS ObjectiveAssessmentPerformanceLevel_Indicator
FROM UnpivotedScores


		
INSERT INTO [EdFiDW].[dbo].[DimAssessment]
           ([_sourceKey]
           ,[AssessmentCategoryDescriptor_CodeValue]
           ,[AssessmentCategoryDescriptor_Description]
           ,[AssessmentFamilyTitle]
           ,[AdaptiveAssessment_Indicator]
           ,[AssessmentIdentifier]
           ,[AssessmentTitle]

           ,[ReportingMethodDescriptor_CodeValue]
           ,[ReportingMethodDescriptor_Description]

           ,[ResultDatatypeTypeDescriptor_CodeValue]
           ,[ResultDatatypeTypeDescriptor_Description]

           ,[AssessmentScore_Indicator]
           ,[AssessmentPerformanceLevel_Indicator]

           ,[ObjectiveAssessmentScore_Indicator]
           ,[ObjectiveAssessmentPerformanceLevel_Indicator]

           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])

    
SELECT DISTINCT 
       'LegacyDW|' + Convert(NVARCHAR(MAX),AssessmentIdentifier)  + '|' + Convert(NVARCHAR(MAX),ObjectiveAssessmentIdentificationCode) + '|' + Convert(NVARCHAR(MAX),ReportingMethodDescriptor_CodeValue)   AS [_sourceKey]
       
	   ,[AssessmentCategoryDescriptor_CodeValue]
       ,[AssessmentCategoryDescriptor_Description]
       ,[AssessmentFamilyTitle]
       ,[AdaptiveAssessment_Indicator]
       ,[AssessmentIdentifier]
       ,[AssessmentTitle]

       ,[ReportingMethodDescriptor_CodeValue]
       ,[ReportingMethodDescriptor_Description]

       ,[ResultDatatypeTypeDescriptor_CodeValue]
       ,[ResultDatatypeTypeDescriptor_Description]

       ,[AssessmentScore_Indicator]
       ,[AssessmentPerformanceLevel_Indicator]

       ,[ObjectiveAssessmentScore_Indicator]
       ,[ObjectiveAssessmentPerformanceLevel_Indicator],
	  
	   GETDATE() AS ValidFrom,
	   '12/31/9999' as ValidTo,
	    1 AS IsCurrent,
	   @lineageKey AS [LineageKey]
FROM @Assessment



--select * from EdFiDW.[dbo].[DimAssessment]

--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;





