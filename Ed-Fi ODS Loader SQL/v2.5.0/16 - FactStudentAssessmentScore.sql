DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM EdFiDW.[dbo].[Lineage] WHERE TableName= 'dbo.FactStudentAssessmentScore')
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
	 FROM EdFiDW.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.FactStudentAssessmentScore'
END 

truncate table EdFiDW.[dbo].[FactStudentAssessmentScore]


 --Assessment Scores 
INSERT INTO EdFiDW.[dbo].[FactStudentAssessmentScore]
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
	  sas.Result AS [SoreResult],
	  CASE when ascr_rdtt.CodeValue in ('Integer') AND TRY_CAST(sas.Result AS INTEGER) IS NOT NULL AND sas.Result <> '-' THEN sas.Result ELSE NULL END AS IntegerScoreResult,
	  CASE when ascr_rdtt.CodeValue in ('Decimal','Percentage','Percentile')  AND TRY_CAST(sas.Result AS FLOAT)  IS NOT NULL THEN sas.Result ELSE NULL END AS DecimalScoreResult,
	  CASE when ascr_rdtt.CodeValue not in ('Integer','Decimal','Percentage','Percentile') THEN sas.Result ELSE NULL END AS LiteralScoreResult,
      @lineageKey AS [LineageKey]
--select top 1 'Ed-Fi|' + Convert(NVARCHAR(MAX),sa.AssessmentIdentifier)  + '|N/A|' + Convert(NVARCHAR(MAX),armt.CodeValue), sa.AdministrationDate,*  
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].Student s 
      
	--student assessment
	inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].StudentAssessment sa on sa.StudentUSI = s.StudentUSI
      
	--student assessment score results
	inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].StudentAssessmentScoreResult sas on sa.StudentAssessmentIdentifier = sas.StudentAssessmentIdentifier
													and sa.AssessmentIdentifier = sas.AssessmentIdentifier

	--assessment 
	inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].Assessment a on sa.AssessmentIdentifier = a.AssessmentIdentifier 

	inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].AssessmentScore ascr on sas.AssessmentIdentifier = ascr.AssessmentIdentifier 
										and sas.[AssessmentReportingMethodTypeId] = ascr.[AssessmentReportingMethodTypeId]
	inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].[AssessmentReportingMethodType] armt on ascr.[AssessmentReportingMethodTypeId] = armt.[AssessmentReportingMethodTypeId]

	INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ResultDatatypeType ascr_rdtt ON ascr.ResultDatatypeTypeId = ascr_rdtt.ResultDatatypeTypeId
	
	--joining DW tables
	INNER JOIN EdFiDW.dbo.DimTime dt ON CONVERT(DATE ,sa.AdministrationDate) = dt.SchoolDate
	
	INNER JOIN EdFiDW.dbo.DimStudent ds  ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),s.StudentUSI)   = ds._sourceKey
	                                     AND dt.SchoolDate BETWEEN ds.ValidFrom AND ds.ValidTo										 
										 AND dt.SchoolKey = ds.SchoolKey
	                               
    INNER JOIN EdFiDW.[dbo].DimAssessment da ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),sa.AssessmentIdentifier)  + '|N/A|' + Convert(NVARCHAR(MAX),armt.CodeValue)  = da._sourceKey
	
WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1 
     AND sa.AdministrationDate >= '07/01/2018'
	
--Assessment Performance Levels
INSERT INTO EdFiDW.[dbo].[FactStudentAssessmentScore]
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
	  apl_ld.CodeValue AS [SoreResult],
	  NULL AS IntegerScoreResult,
	  NULL AS DecimalScoreResult,
	  apl_ld.CodeValue AS LiteralScoreResult,	  
      @lineageKey AS [LineageKey]
--select top 100 *  
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].Student s 
      
	--student assessment
	inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].StudentAssessment sa on sa.StudentUSI = s.StudentUSI 
	
	inner  join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].StudentAssessmentPerformanceLevel sapl on sa.StudentAssessmentIdentifier = sapl.StudentAssessmentIdentifier
	                                                         and sa.AssessmentIdentifier = sapl.AssessmentIdentifier
												 --    and apl.PerformanceLevelDescriptorId = sapl.PerformanceLevelDescriptorId
    
	--assessment 
	inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].Assessment a on sa.AssessmentIdentifier = a.AssessmentIdentifier 

    inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].[AssessmentPerformanceLevel] apl on sa.AssessmentIdentifier = apl.AssessmentIdentifier 
	                                                 and sapl.[AssessmentReportingMethodTypeId] = apl.[AssessmentReportingMethodTypeId]
											         and sapl.PerformanceLevelDescriptorId = apl.PerformanceLevelDescriptorId
    
	inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].[AssessmentReportingMethodType] apl_sd on apl.[AssessmentReportingMethodTypeId] = apl_sd.[AssessmentReportingMethodTypeId] 
	inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].Descriptor apl_ld on apl.PerformanceLevelDescriptorId = apl_ld.DescriptorId 

	--joining DW tables
	INNER JOIN EdFiDW.dbo.DimTime dt ON CONVERT(DATE ,sa.AdministrationDate) = dt.SchoolDate
	INNER JOIN EdFiDW.dbo.DimStudent ds  ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),s.StudentUSI)   = ds._sourceKey
	                                     AND dt.SchoolDate BETWEEN ds.ValidFrom AND ds.ValidTo

										 AND dt.SchoolKey = ds.SchoolKey	                               
    INNER JOIN EdFiDW.[dbo].DimAssessment da ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),sa.AssessmentIdentifier)  + '|N/A|' + Convert(NVARCHAR(MAX),apl_sd.CodeValue)  = da._sourceKey

WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1           
	 AND sa.AdministrationDate >= '07/18/2018'
	
	
--select * from EdFiDW.[dbo].[FactStudentAssessmentScore]

--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;


/*

CREATE NONCLUSTERED INDEX [IX_DimTime_SchoolDate_AndKey_Covering_All]
ON [dbo].[DimTime] ([SchoolDate],[SchoolKey])
INCLUDE ([SchoolDate_MMYYYY],[SchoolDate_Fomat1],[SchoolDate_Fomat2],[SchoolDate_Fomat3],[SchoolYear],[SchoolYearDescription],[CalendarYear],[DayOfMonth],[DaySuffix],[DayName],[DayNameShort],[DayOfWeek],[WeekInMonth],[WeekOfMonth],[Weekend_Indicator],[WeekOfYear],[FirstDayOfWeek],[LastDayOfWeek],[WeekBeforeChristmas_Indicator],[Month],[MonthName],[MonthNameShort],[FirstDayOfMonth],[LastDayOfMonth],[FirstDayOfNextMonth],[LastDayOfNextMonth],[DayOfYear],[LeapYear_Indicator],[FederalHolidayName],[FederalHoliday_Indicator],[DayOfSchoolYear],[SchoolCalendarEventType_CodeValue],[SchoolCalendarEventType_Description],[SchoolTermDescriptor_CodeValue],[SchoolTermDescriptor_Description],[ValidFrom],[ValidTo],[IsCurrent],[LineageKey])

CREATE NONCLUSTERED INDEX [IX_DimStudent_SourceKey]
ON [dbo].[DimStudent] ([_sourceKey])

CREATE NONCLUSTERED INDEX [IX_DimStudent_ValidFrom_ValidTo]
ON [dbo].[DimStudent] ([ValidFrom], [ValidTo])


CREATE NONCLUSTERED INDEX [IX_DimAssessment_SourceKey]
ON [dbo].[DimAssessment] ([_sourceKey])


CREATE NONCLUSTERED INDEX [IX_DimTime_SchoolDate]
ON [dbo].[DimTime] ([SchoolDate])

*/
