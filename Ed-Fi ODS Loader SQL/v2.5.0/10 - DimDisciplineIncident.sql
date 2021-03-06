DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM EdFiDW.[dbo].[Lineage] WHERE TableName= 'dbo.DimDisciplineIncident')
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
	('dbo.DimDisciplineIncident', 
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
	 WHERE TableName= 'dbo.DimDisciplineIncident'
END 


--DELETE FROM EdFiDW.[dbo].DimDisciplineIncident
--WHERE SchoolYear = EdFiDW.dbo.Func_GetSchoolYear(GETDATE())
         

INSERT INTO EdFiDW.[dbo].DimDisciplineIncident
             (_sourceKey
			 ,[SchoolKey]
             ,[ShortNameOfInstitution]
             ,[NameOfInstitution]
             ,[SchoolYear]
             ,[IncidentDate]
             ,[IncidentTime]
			 ,[IncidentDescription]
             ,[BehaviorDescriptor_CodeValue]
             ,[BehaviorDescriptor_Description]
             ,[LocationDescriptor_CodeValue]
             ,[LocationDescriptor_Description]
             ,[DisciplineDescriptor_CodeValue]
             ,[DisciplineDescriptor_Description]
             ,DisciplineDescriptor_ISS_Indicator
			 ,DisciplineDescriptor_OSS_Indicator
			 ,[ReporterDescriptor_CodeValue]
             ,[ReporterDescriptor_Description]
			 
             ,[IncidentReporterName]
             ,[ReportedToLawEnforcement_Indicator]
             ,[IncidentCost]
             ,[LineageKey])
SELECT DISTINCT 
      CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),di.IncidentIdentifier)) AS [_sourceKey],
      dschool.SchoolKey,
	  dschool.ShortNameOfInstitution,
	  dschool.NameOfInstitution,
	  EdFiDW.dbo.Func_GetSchoolYear(di.IncidentDate),
	  di.IncidentDate,
	  COALESCE(di.IncidentTime,'00:00:00.0000000') AS IncidentTime,
	  di.IncidentDescription,
	  COALESCE(d_dib.CodeValue,'N/A') as [BehaviorDescriptor_CodeValue],
	  COALESCE(d_dib.Description,'N/A') as [BehaviorDescriptor_Description],
	  
	  COALESCE(d_dil.CodeValue,'N/A') as [LocationDescriptor_CodeValue],
	  COALESCE(d_dil.Description,'N/A') as [LocationDescriptor_Description],

	  COALESCE(d_dia.CodeValue,'N/A') as [DisciplineDescriptor_CodeValue],
	  COALESCE(d_dia.Description,'N/A') as [DisciplineDescriptor_Description],
	  CASE WHEN  COALESCE(d_dia.CodeValue,'N/A') IN ('In School Suspension','In-School Suspension') THEN 1 ELSE 0 END as DisciplineDescriptor_ISS_Indicator,
	  CASE WHEN  COALESCE(d_dia.CodeValue,'N/A') IN ('Out of School Suspension','Out-Of-School Suspension') THEN 1 ELSE 0 END as DisciplineDescriptor_OSS_Indicator,
	  
	  COALESCE(d_dirt.CodeValue,'N/A') as ReporterDescriptor_CodeValue,
	  COALESCE(d_dirt.Description,'N/A') as ReporterDescriptor_Description,

	  COALESCE(di.ReporterName,'N/A'),
	  COALESCE(di.ReportedToLawEnforcement,0) AS ReportedToLawEnforcement,
	  COALESCE(di.IncidentCost,0) AS IncidentCost,

	  @lineageKey AS [LineageKey]
FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncident di       
	  LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncidentBehavior dib ON di.IncidentIdentifier = dib.IncidentIdentifier
      LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineActionDisciplineIncident dadi ON di.IncidentIdentifier = dadi.IncidentIdentifier
	  LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineActionDiscipline dad ON dadi.DisciplineActionIdentifier = dad.DisciplineActionIdentifier

	  INNER JOIN EdFiDW.dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),di.SchoolId)   = dschool._sourceKey
	  INNER JOIN EdFiDW.dbo.DimTime dt ON di.IncidentDate = dt.SchoolDate
	                                  AND dt.SchoolKey is not null   
									  AND dschool.SchoolKey = dt.SchoolKey
	  LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d_dib ON dib.BehaviorDescriptorId   = d_dib.DescriptorId
	  LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.IncidentLocationType d_dil ON di.IncidentLocationTypeId   = d_dil.IncidentLocationTypeId
	  LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d_dia ON dad.DisciplineDescriptorId   = d_dia.DescriptorId
	  LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d_dirt ON di.ReporterDescriptionDescriptorId   = d_dirt.DescriptorId
WHERE EdFiDW.dbo.Func_GetSchoolYear(di.IncidentDate) IN (2019,2020)

--select * from EdFiDW.[dbo].[DimDisciplineIncident]



--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;

