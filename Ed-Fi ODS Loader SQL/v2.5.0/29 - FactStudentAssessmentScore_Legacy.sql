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
	SELECT testid,schyear,testtime,studentno,adminyear,grade, scoretype, scorevalue,lastupdate, CASE WHEN a.scoretype IN ('Proficiency level','Proficiency level 2') THEN 1 ELSE 0 END AS isperflevel
	--INTO [Raw_LegacyDW].[MCASAssessmentScores]
	FROM (  
			 --ensuring all score columns have the same data type to avoid conflicts with unpivot
			 SELECT testid,schyear,testtime,studentno,adminyear,grade,teststatus , lastupdate,
				  CAST(rawscore AS NVARCHAR(MAX)) AS [Raw score],
				  CAST(scaledscore AS NVARCHAR(MAX)) AS [Scale score],
				  CAST(perflevel AS NVARCHAR(MAX)) AS  [Proficiency level],
				  CAST(sgp AS NVARCHAR(MAX)) AS [Percentile rank],
				  CAST(cpi AS NVARCHAR(MAX)) AS [Composite Performance Index],
				  CAST(perf2 AS NVARCHAR(MAX)) AS [Proficiency level 2]
     
			FROM [RAEDatabase].[dbo].[mcasitems] 
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
	
	INNER JOIN LongitudinalPOC.dbo.DimTime dt ON CONVERT(DATE ,CASE WHEN SUBSTRING(us.testid,LEN(us.testid)-1,1) IN ('E','X') THEN us.lastupdate
	                                                                WHEN us.testid = 'MCAS03AE' and us.testtime = 'S' and us.schyear = '2015' then '3/21/2016'
																	WHEN us.testid = 'MCAS03AM' and us.testtime = 'S' and us.schyear = '2015' then '5/9/2016'
																	WHEN us.testid = 'MCAS04AE' and us.testtime = 'S' and us.schyear = '2015' then '3/22/2016'
																	WHEN us.testid = 'MCAS04AM' and us.testtime = 'S' and us.schyear = '2015' then '5/9/2016'
																	WHEN us.testid = 'MCAS05AE' and us.testtime = 'S' and us.schyear = '2015' then '3/21/2016'
																	WHEN us.testid = 'MCAS05AM' and us.testtime = 'S' and us.schyear = '2015' then '5/9/2016'
																	WHEN us.testid = 'MCAS05AS' and us.testtime = 'S' and us.schyear = '2015' then '5/10/2016'
																	WHEN us.testid = 'MCAS06AE' and us.testtime = 'S' and us.schyear = '2015' then '3/21/2016'
																	WHEN us.testid = 'MCAS06AM' and us.testtime = 'S' and us.schyear = '2015' then '5/9/2016'
																	WHEN us.testid = 'MCAS07AE' and us.testtime = 'S' and us.schyear = '2015' then '3/22/2016'
																	WHEN us.testid = 'MCAS07AM' and us.testtime = 'S' and us.schyear = '2015' then '5/9/2016'
																	WHEN us.testid = 'MCAS08AE' and us.testtime = 'S' and us.schyear = '2015' then '3/21/2016'
																	WHEN us.testid = 'MCAS08AM' and us.testtime = 'S' and us.schyear = '2015' then '5/9/2016'
																	WHEN us.testid = 'MCAS08AS' and us.testtime = 'S' and us.schyear = '2015' then '5/10/2016'
																	WHEN us.testid = 'MCAS10AE' and us.testtime = 'S' and us.schyear = '2015' then '3/22/2016'
																	WHEN us.testid = 'MCAS10AM' and us.testtime = 'S' and us.schyear = '2015' then '3/23/2016'
																	WHEN us.testid = 'MCAS10BE' and us.testtime = 'F' and us.schyear = '2015' then '11/4/2015'
																	WHEN us.testid = 'MCAS10BE' and us.testtime = 'W' and us.schyear = '2015' then '3/2/2016'
																	WHEN us.testid = 'MCAS10BM' and us.testtime = 'F' and us.schyear = '2015' then '11/9/2015'
																	WHEN us.testid = 'MCAS10BM' and us.testtime = 'W' and us.schyear = '2015' then '3/2/2016'
																	WHEN us.testid = 'MCASHSAB' and us.testtime = 'W' and us.schyear = '2015' then '2/1/2016'
																	WHEN us.testid = 'MCASHSAB' and us.testtime = 'S' and us.schyear = '2015' then '6/1/2016'
																	WHEN us.testid = 'MCASHSAC' and us.testtime = 'S' and us.schyear = '2015' then '6/1/2016'
																	WHEN us.testid = 'MCASHSAP' and us.testtime = 'S' and us.schyear = '2015' then '6/1/2016'
																	WHEN us.testid = 'MCASHSAT' and us.testtime = 'S' and us.schyear = '2015' then '6/1/2016'
																	WHEN us.testid = 'MCAS03AE' and us.testtime = 'S' and us.schyear = '2016' then '4/3/2017'
																	WHEN us.testid = 'MCAS03AM' and us.testtime = 'S' and us.schyear = '2016' then '4/4/2017'
																	WHEN us.testid = 'MCAS04AE' and us.testtime = 'S' and us.schyear = '2016' then '4/3/2017'
																	WHEN us.testid = 'MCAS04AM' and us.testtime = 'S' and us.schyear = '2016' then '4/4/2017'
																	WHEN us.testid = 'MCAS05AE' and us.testtime = 'S' and us.schyear = '2016' then '4/3/2017'
																	WHEN us.testid = 'MCAS05AM' and us.testtime = 'S' and us.schyear = '2016' then '4/4/2017'
																	WHEN us.testid = 'MCAS05AS' and us.testtime = 'S' and us.schyear = '2016' then '4/5/2017'
																	WHEN us.testid = 'MCAS06AE' and us.testtime = 'S' and us.schyear = '2016' then '4/3/2017'
																	WHEN us.testid = 'MCAS06AM' and us.testtime = 'S' and us.schyear = '2016' then '4/4/2017'
																	WHEN us.testid = 'MCAS07AE' and us.testtime = 'S' and us.schyear = '2016' then '4/3/2017'
																	WHEN us.testid = 'MCAS07AM' and us.testtime = 'S' and us.schyear = '2016' then '4/4/2017'
																	WHEN us.testid = 'MCAS08AE' and us.testtime = 'S' and us.schyear = '2016' then '4/3/2017'
																	WHEN us.testid = 'MCAS08AM' and us.testtime = 'S' and us.schyear = '2016' then '4/4/2017'
																	WHEN us.testid = 'MCAS08AS' and us.testtime = 'S' and us.schyear = '2016' then '4/5/2017'
																	WHEN us.testid = 'MCAS10AE' and us.testtime = 'S' and us.schyear = '2016' then '3/21/2017'
																	WHEN us.testid = 'MCAS10AM' and us.testtime = 'S' and us.schyear = '2016' then '5/16/2017'
																	WHEN us.testid = 'MCAS10BE' and us.testtime = 'F' and us.schyear = '2016' then '11/2/2016'
																	WHEN us.testid = 'MCAS10BE' and us.testtime = 'W' and us.schyear = '2016' then '3/1/2017'
																	WHEN us.testid = 'MCAS10BM' and us.testtime = 'F' and us.schyear = '2016' then '11/9/2016'
																	WHEN us.testid = 'MCAS10BM' and us.testtime = 'W' and us.schyear = '2016' then '3/1/2017'
																	WHEN us.testid = 'MCASHSAB' and us.testtime = 'W' and us.schyear = '2016' then '2/6/2017'
																	WHEN us.testid = 'MCASHSAB' and us.testtime = 'S' and us.schyear = '2016' then '6/5/2017'
																	WHEN us.testid = 'MCASHSAC' and us.testtime = 'S' and us.schyear = '2016' then '6/5/2017'
																	WHEN us.testid = 'MCASHSAP' and us.testtime = 'S' and us.schyear = '2016' then '6/5/2017'
																	WHEN us.testid = 'MCASHSAT' and us.testtime = 'S' and us.schyear = '2016' then '6/5/2017'
																	WHEN us.testid = 'MCAS03AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																	WHEN us.testid = 'MCAS03AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																	WHEN us.testid = 'MCAS04AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																	WHEN us.testid = 'MCAS04AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																	WHEN us.testid = 'MCAS05AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																	WHEN us.testid = 'MCAS05AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																	WHEN us.testid = 'MCAS05AS' and us.testtime = 'S' and us.schyear = '2017' then '4/4/2018'
																	WHEN us.testid = 'MCAS06AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																	WHEN us.testid = 'MCAS06AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																	WHEN us.testid = 'MCAS07AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																	WHEN us.testid = 'MCAS07AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																	WHEN us.testid = 'MCAS08AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																	WHEN us.testid = 'MCAS08AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																	WHEN us.testid = 'MCAS08AS' and us.testtime = 'S' and us.schyear = '2017' then '4/4/2018'
																	WHEN us.testid = 'MCAS10AE' and us.testtime = 'S' and us.schyear = '2017' then '3/27/2018'
																	WHEN us.testid = 'MCAS10AM' and us.testtime = 'S' and us.schyear = '2017' then '5/23/2018'
																	WHEN us.testid = 'MCAS10BE' and us.testtime = 'F' and us.schyear = '2017' then '11/8/2017'
																	WHEN us.testid = 'MCAS10BE' and us.testtime = 'W' and us.schyear = '2017' then '2/28/2018'
																	WHEN us.testid = 'MCAS10BM' and us.testtime = 'W' and us.schyear = '2017' then '2/28/2018'
																	WHEN us.testid = 'MCAS10BM' and us.testtime = 'F' and us.schyear = '2017' then '11/15/2017'
																	WHEN us.testid = 'MCAS3 AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																	WHEN us.testid = 'MCAS3 AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																	WHEN us.testid = 'MCAS4 AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																	WHEN us.testid = 'MCAS4 AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																	WHEN us.testid = 'MCAS5 AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																	WHEN us.testid = 'MCAS5 AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																	WHEN us.testid = 'MCAS5 AS' and us.testtime = 'S' and us.schyear = '2017' then '4/4/2018'
																	WHEN us.testid = 'MCASHSAB' and us.testtime = 'S' and us.schyear = '2017' then '2/1/2018'
																	WHEN us.testid = 'MCASHSAB' and us.testtime = 'W' and us.schyear = '2017' then '6/1/2018'
																	WHEN us.testid = 'MCASHSAC' and us.testtime = 'S' and us.schyear = '2017' then '6/1/2018'
																	WHEN us.testid = 'MCASHSAP' and us.testtime = 'S' and us.schyear = '2017' then '6/1/2018'
																	WHEN us.testid = 'MCASHSAT' and us.testtime = 'S' and us.schyear = '2017' then '6/1/2018'
																	ELSE '1900/01/01'
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

select testid, SUBSTRING(testid,LEN(testid)-1,1) , * FROM [RAEDatabase].[dbo].[mcasitems] WHERE testid IN ('MCASHSET','MCASHSXB')

	