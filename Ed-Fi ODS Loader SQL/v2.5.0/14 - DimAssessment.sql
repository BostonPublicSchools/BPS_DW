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
	   a_d.CodeValue AS [AssessmentCategoryDescriptor_CodeValue],
	   a_d.[Description] AS [AssessmentCategoryDescriptor_Description],
	   a.AssessmentFamilyTitle AS [AssessmentFamilyTitle],
	   ISNULL(a.AdaptiveAssessment,0) AS [AdaptiveAssessment_Indicator], 
	   a.AssessmentIdentifier,
	   'N/A' AS ObjectiveAssessmentIdentificationCode,
	   a.AssessmentTitle,	   

       a_s_armt.CodeValue AS ReportingMethodDescriptor_CodeValue,
	   a_s_armt.[Description] AS ReportingMethodDescriptor_Description,
	   a_s_rdtt.CodeValue AS ResultDatatypeTypeDescriptor_CodeValue,
	   a_s_rdtt.[Description] AS ResultDatatypeTypeDescriptor_Description,

	   1 AS AssessmentScore_Indicator,
	   0 AS AssessmentPerformanceLevel_Indicator,
  
	   0 AS ObjectiveAssessmentScore_Indicator,
	   0 AS ObjectiveAssessmentPerformanceLevel_Indicator


FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Assessment a 
     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor a_d ON a.AssessmentCategoryDescriptorId = a_d.DescriptorId
	 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.AssessmentScore a_s ON a.AssessmentIdentifier = a_s.AssessmentIdentifier 
	 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.AssessmentReportingMethodType a_s_armt ON a_s.AssessmentReportingMethodTypeId = a_s_armt.AssessmentReportingMethodTypeId
	 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ResultDatatypeType a_s_rdtt ON a_s.ResultDatatypeTypeId = a_s_rdtt.ResultDatatypeTypeId
WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1 
Union
SELECT DISTINCT 
	   a_d.CodeValue AS [AssessmentCategoryDescriptor_CodeValue],
	   a_d.[Description] AS [AssessmentCategoryDescriptor_Description],
	   a.AssessmentFamilyTitle AS [AssessmentFamilyTitle],
	   ISNULL(a.AdaptiveAssessment,0) AS [AdaptiveAssessment_Indicator], 
	   a.AssessmentIdentifier,
	    'N/A' AS ObjectiveAssessmentIdentificationCode,
	   a.AssessmentTitle,	   
	   
	   a_pl_armt.CodeValue AS ReportingMethodDescriptor_CodeValue,
	   a_pl_armt.[Description] AS ReportingMethodDescriptor_Description,
	   a_pl_rdtt.CodeValue AS ResultDatatypeTypeDescriptor_CodeValue,
	   a_pl_rdtt.[Description] AS ResultDatatypeTypeDescriptor_Description,

	   0 AS AssessmentScore_Indicator,
	   1 AS AssessmentPerformanceLevel_Indicator,
  
	   0 AS ObjectiveAssessmentScore_Indicator,
	   0 AS ObjectiveAssessmentPerformanceLevel_Indicator


FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Assessment a 
     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor a_d ON a.AssessmentCategoryDescriptorId = a_d.DescriptorId
	 
	 
	 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.AssessmentPerformanceLevel a_pl ON a.AssessmentIdentifier = a_pl.AssessmentIdentifier 
	 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor a_pl_d ON a_pl.PerformanceLevelDescriptorId = a_pl_d.DescriptorId
	 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.AssessmentReportingMethodType a_pl_armt ON a_pl.AssessmentReportingMethodTypeId = a_pl_armt.AssessmentReportingMethodTypeId
	 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ResultDatatypeType a_pl_rdtt ON a_pl.ResultDatatypeTypeId = a_pl_rdtt.ResultDatatypeTypeId
WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1 
ORDER BY a.AssessmentIdentifier, ObjectiveAssessmentIdentificationCode, ReportingMethodDescriptor_CodeValue
	 
		
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
       'Ed-Fi|' + Convert(NVARCHAR(MAX),AssessmentIdentifier)  + '|' + Convert(NVARCHAR(MAX),ObjectiveAssessmentIdentificationCode) + '|' + Convert(NVARCHAR(MAX),ReportingMethodDescriptor_CodeValue)   AS [_sourceKey]
       
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





