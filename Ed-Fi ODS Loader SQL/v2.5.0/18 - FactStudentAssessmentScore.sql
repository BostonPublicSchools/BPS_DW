DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM BPS_DW.[dbo].[Lineage] WHERE TableName= 'dbo.FactStudentAssessmentScore')
BEGIN
    INSERT INTO BPS_DW.[dbo].[Lineage]
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
	 FROM BPS_DW.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.FactStudentAssessmentScore'
END 

truncate table BPS_DW.[dbo].[FactStudentAssessmentScore]


 --Assessment Scores 
INSERT INTO BPS_DW.[dbo].[FactStudentAssessmentScore]
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
	  --sa.AdministrationDate,
	 -- ds.ValidFrom,
	 -- ds.ValidTo
      @lineageKey AS [LineageKey]
--select top 100 *  
FROM [EdFi_BPS_Staging_Ods].[edfi].Student s 
      
	--student assessment
	inner join [EdFi_BPS_Staging_Ods].[edfi].StudentAssessment sa on sa.StudentUSI = s.StudentUSI
      
	--student assessment score results
	inner join [EdFi_BPS_Staging_Ods].[edfi].StudentAssessmentScoreResult sas on sa.StudentAssessmentIdentifier = sas.StudentAssessmentIdentifier
													and sa.AssessmentIdentifier = sas.AssessmentIdentifier

	--assessment 
	inner join [EdFi_BPS_Staging_Ods].[edfi].Assessment a on sa.AssessmentIdentifier = a.AssessmentIdentifier 

	inner join [EdFi_BPS_Staging_Ods].[edfi].AssessmentScore ascr on sas.AssessmentIdentifier = ascr.AssessmentIdentifier 
										and sas.[AssessmentReportingMethodTypeId] = ascr.[AssessmentReportingMethodTypeId]
	inner join [EdFi_BPS_Staging_Ods].[edfi].[AssessmentReportingMethodType] armt on ascr.[AssessmentReportingMethodTypeId] = armt.[AssessmentReportingMethodTypeId]

	INNER JOIN EdFi_BPS_Staging_Ods.edfi.ResultDatatypeType ascr_rdtt ON ascr.ResultDatatypeTypeId = ascr_rdtt.ResultDatatypeTypeId

	--joining DW tables
	INNER JOIN BPS_DW.dbo.DimStudent ds  ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),s.StudentUSI)   = ds._sourceKey
	
	INNER JOIN BPS_DW.dbo.DimTime dt ON CONVERT(DATE ,sa.AdministrationDate) = dt.SchoolDate
	                               
    INNER JOIN BPS_DW.[dbo].DimAssessment da ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),sa.AssessmentIdentifier)  + '|N/A|' + Convert(NVARCHAR(MAX),armt.CodeValue)  = da._sourceKey
	
	

WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1 
     AND ds.IsCurrent =1;
     -- AND sa.AdministrationDate BETWEEN ds.ValidFrom AND da.ValidTo
	  
--Assessment Performance Levels
INSERT INTO BPS_DW.[dbo].[FactStudentAssessmentScore]
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
	  --sa.AdministrationDate,
	 -- ds.ValidFrom,
	 -- ds.ValidTo
      @lineageKey AS [LineageKey]
--select top 100 *  
FROM [EdFi_BPS_Staging_Ods].[edfi].Student s 
      
	--student assessment
	inner join [EdFi_BPS_Staging_Ods].[edfi].StudentAssessment sa on sa.StudentUSI = s.StudentUSI 
	
	inner  join [EdFi_BPS_Staging_Ods].[edfi].StudentAssessmentPerformanceLevel sapl on sa.StudentAssessmentIdentifier = sapl.StudentAssessmentIdentifier
	                                                         and sa.AssessmentIdentifier = sapl.AssessmentIdentifier
												 --    and apl.PerformanceLevelDescriptorId = sapl.PerformanceLevelDescriptorId
    
	--assessment 
	inner join [EdFi_BPS_Staging_Ods].[edfi].Assessment a on sa.AssessmentIdentifier = a.AssessmentIdentifier 

    inner join [EdFi_BPS_Staging_Ods].[edfi].[AssessmentPerformanceLevel] apl on sa.AssessmentIdentifier = apl.AssessmentIdentifier 
	                                                 and sapl.[AssessmentReportingMethodTypeId] = apl.[AssessmentReportingMethodTypeId]
											         and sapl.PerformanceLevelDescriptorId = apl.PerformanceLevelDescriptorId
    
	inner join [EdFi_BPS_Staging_Ods].[edfi].[AssessmentReportingMethodType] apl_sd on apl.[AssessmentReportingMethodTypeId] = apl_sd.[AssessmentReportingMethodTypeId] 
	inner join [EdFi_BPS_Staging_Ods].[edfi].Descriptor apl_ld on apl.PerformanceLevelDescriptorId = apl_ld.DescriptorId 

	--joining DW tables
	INNER JOIN BPS_DW.dbo.DimStudent ds  ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),s.StudentUSI)   = ds._sourceKey
	
	INNER JOIN BPS_DW.dbo.DimTime dt ON CONVERT(DATE ,sa.AdministrationDate) = dt.SchoolDate
	                               
    INNER JOIN BPS_DW.[dbo].DimAssessment da ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),sa.AssessmentIdentifier)  + '|N/A|' + Convert(NVARCHAR(MAX),apl_sd.CodeValue)  = da._sourceKey
	
	

WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1 
     AND ds.IsCurrent =1 
	

--updatng the lineage table
UPDATE BPS_DW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;

/*
 [EdFi_BPS_Staging_Ods]
 SELECT * FROM EdFi_BPS_Staging_Ods.edfi.ResultDatatypeType
 SELECT CONVERT(DATE ,a.AdministrationDate) FROM  [EdFi_BPS_Staging_Ods].[edfi].StudentAssessment a WHERE  CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1 
 SELECT * FROM  BPS_DW.dbo.DimTime WHERE SchoolDate = '2018-04-02'
*/

