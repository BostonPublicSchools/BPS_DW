DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM LongitudinalPOC.[dbo].[Lineage] WHERE TableName= 'dbo.FactStudentAssessmentScore')
BEGIN
    INSERT INTO LongitudinalPOC.[dbo].[Lineage]
	(
	 [TableName], 
	 [StartTime], 
	 [EndTime], 
	 [LoadType], 
	 [Status]
	)
	VALUES
	('dbo.FactStudentAssessmentScore', 
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
	 FROM LongitudinalPOC.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.FactStudentAssessmentScore'
END 

--truncate table LongitudinalPOC.[dbo].[FactStudentAssessmentScore]


 --Assessment Scores 
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
				  CAST(cpi AS NVARCHAR(MAX)) AS [Composite Rating],
				  CAST(perf2 AS NVARCHAR(MAX)) AS [Proficiency level 2]
     
			FROM [RAEDatabase].[dbo].[mcasitems] 
			WHERE schyear >= 2015 ) scores
	UNPIVOT
	(
	   scorevalue
	   FOR scoretype IN ([Raw score],[Scale score],[Proficiency level],[Percentile rank],[Composite Rating],[Proficiency level 2])
	) AS a

--Composite Rating
--Composite Score ?
--Percentile rank
--Promotion score ?
--Raw score
--Scale score
--Proficiency level

)

INSERT INTO LongitudinalPOC.[dbo].[FactStudentAssessmentScore]
           ([StudentKey]
           ,[TimeKey]
           ,[AssessmentKey]
		   ,ScoreResult
           ,IntegerScoreResult
           ,DecimalScoreResult
           ,LiteralScoreResult
           ,[LineageKey])

SELECT   DISTINCT 
      ds.StudentKey,
      dt.TimeKey,	  
	  da.AssessmentKey,
	  us.scorevalue AS [SoreResult],
	  CASE when da.ResultDatatypeTypeDescriptor_CodeValue in ('Integer') AND TRY_CAST(us.scorevalue AS INTEGER) IS NOT NULL AND us.scorevalue <> '-' THEN us.scorevalue ELSE NULL END AS IntegerScoreResult,
	  CASE when da.ResultDatatypeTypeDescriptor_CodeValue in ('Decimal','Percentage','Percentile')  AND TRY_CAST(us.scorevalue AS FLOAT)  IS NOT NULL THEN us.scorevalue ELSE NULL END AS DecimalScoreResult,
	  CASE when da.ResultDatatypeTypeDescriptor_CodeValue not in ('Integer','Decimal','Percentage','Percentile') THEN us.scorevalue ELSE NULL END AS LiteralScoreResult,
	  --sa.AdministrationDate,	
      @lineageKey AS [LineageKey]
--select top 100 *  
FROM UnpivotedScores us

	--joining DW tables
	INNER JOIN LongitudinalPOC.dbo.DimStudent ds  ON 'LegacyDW|' + Convert(NVARCHAR(MAX),us.studentno)   = ds._sourceKey
	
	INNER JOIN LongitudinalPOC.dbo.DimTime dt ON CONVERT(DATE ,CASE us.testtime 
	                                                                WHEN 'S' THEN CONCAT('05/01/',us.adminyear)
																	WHEN 'W' THEN CONCAT('05/01/',us.adminyear)
																	WHEN 'F' THEN CONCAT('05/01/',us.adminyear)
																	WHEN 'U' THEN CONCAT('05/01/',us.adminyear)
																	ELSE  CONCAT('05/01/',us.adminyear)
															   END ) = dt.SchoolDate
	                               
    INNER JOIN LongitudinalPOC.[dbo].DimAssessment da ON 'LegacyDW|' + Convert(NVARCHAR(MAX),us.testid)  + '|N/A|' + Convert(NVARCHAR(MAX),us.scoretype)  = da._sourceKey
	
WHERE 1=1
     -- AND ds.IsCurrent =1;
     -- AND sa.AdministrationDate BETWEEN ds.ValidFrom AND da.ValidTo
	  

--updatng the lineage table
UPDATE LongitudinalPOC.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;

/*
 [EDFISQL01].[EdFi_BPS_Production_Ods]
 SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ResultDatatypeType
 SELECT CONVERT(DATE ,a.AdministrationDate) FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].StudentAssessment a WHERE  CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1 
 SELECT * FROM  LongitudinalPOC.dbo.DimTime WHERE SchoolDate = '2018-04-02'
*/

