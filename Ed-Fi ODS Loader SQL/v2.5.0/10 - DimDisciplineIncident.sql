DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM BPS_DW.[dbo].[Lineage] WHERE TableName= 'dbo.DimDisciplineIncident')
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
	 FROM BPS_DW.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.DimDisciplineIncident'
END 


DELETE FROM BPS_DW.[dbo].DimDisciplineIncident
WHERE SchoolYear = BPS_DW.dbo.Func_GetSchoolYear(GETDATE())
         

INSERT INTO BPS_DW.[dbo].DimDisciplineIncident
             (_sourceKey
			 ,[SchoolKey]
             ,[ShortNameOfInstitution]
             ,[NameOfInstitution]
             ,[SchoolYear]
             ,[IncidentDate]
             ,[IncidentTime]
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
     'Ed-Fi|' + Convert(NVARCHAR(MAX),di.IncidentIdentifier) AS [_sourceKey],
      dschool.SchoolKey,
	  dschool.ShortNameOfInstitution,
	  dschool.NameOfInstitution,
	  BPS_DW.dbo.Func_GetSchoolYear(di.IncidentDate),
	  di.IncidentDate,
	  ISNULL(di.IncidentTime,'00:00:00.0000000') AS IncidentTime,

	  ISNULL(d_dib.CodeValue,'Other') as [BehaviorDescriptor_CodeValue],
	  ISNULL(d_dib.Description,'Other') as [BehaviorDescriptor_Description],
	  
	  ISNULL(d_dil.CodeValue,'Unknown') as [LocationDescriptor_CodeValue],
	  ISNULL(d_dil.Description,'Unknown') as [LocationDescriptor_Description],

	  ISNULL(d_dia.CodeValue,'Other') as [DisciplineDescriptor_CodeValue],
	  ISNULL(d_dia.Description,'Other') as [DisciplineDescriptor_Description],
	  CASE WHEN  ISNULL(d_dia.CodeValue,'Other') IN ('In School Suspension','In-School Suspension)') THEN 1 ELSE 0 END,
	  CASE WHEN  ISNULL(d_dia.CodeValue,'Other') IN ('Out of School Suspension','Out-Of-School Suspension)') THEN 1 ELSE 0 END,
	  
	  ISNULL(d_dirt.CodeValue,'Other') as ReporterDescriptor_CodeValue,
	  ISNULL(d_dirt.Description,'Other') as ReporterDescriptor_Description,

	  ISNULL(di.ReporterName,'N/A'),
	  ISNULL(di.ReportedToLawEnforcement,0) AS ReportedToLawEnforcement,
	  ISNULL(di.IncidentCost,0) AS IncidentCost,

	  @lineageKey AS [LineageKey]
FROM  EdFi_BPS_Staging_Ods.edfi.DisciplineIncident di       
	  LEFT JOIN EdFi_BPS_Staging_Ods.edfi.DisciplineIncidentBehavior dib ON di.IncidentIdentifier = dib.IncidentIdentifier
      LEFT JOIN EDFi_BPS_Staging_Ods.edfi.DisciplineActionDisciplineIncident dadi ON di.IncidentIdentifier = dadi.IncidentIdentifier
	  LEFT JOIN EdFi_BPS_Staging_Ods.edfi.DisciplineActionDiscipline dad ON dadi.DisciplineActionIdentifier = dad.DisciplineActionIdentifier

	  INNER JOIN BPS_DW.dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),di.SchoolId)   = dschool._sourceKey
	  INNER JOIN BPS_DW.dbo.DimTime dt ON di.IncidentDate = dt.SchoolDate
	                                  AND dt.SchoolKey is not null   
									  AND dschool.SchoolKey = dt.SchoolKey
	  LEFT JOIN EdFi_BPS_Staging_Ods.edfi.Descriptor d_dib ON dib.BehaviorDescriptorId   = d_dib.DescriptorId
	  LEFT JOIN EdFi_BPS_Staging_Ods.edfi.Descriptor d_dil ON di.IncidentLocationTypeId   = d_dil.DescriptorId
	  LEFT JOIN EdFi_BPS_Staging_Ods.edfi.Descriptor d_dia ON dad.DisciplineDescriptorId   = d_dia.DescriptorId
	  LEFT JOIN EdFi_BPS_Staging_Ods.edfi.Descriptor d_dirt ON di.ReporterDescriptionDescriptorId   = d_dirt.DescriptorId
WHERE BPS_DW.dbo.Func_GetSchoolYear(di.IncidentDate) IN (2019,2020)

--select * from BPS_DW.[dbo].[DimDisciplineIncident]

--updatng the lineage table
UPDATE BPS_DW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;


/*
--discilpline type
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.DisciplineIncidentBehavior
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.Descriptor WHERE Namespace LIKE '%behavior%'


--location
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.DisciplineIncident
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.IncidentLocationType


--action
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.DisciplineActionDiscipline
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.Descriptor WHERE Namespace LIKE '%discipline%'


--reporter type
SELECT IncidentDate FROM EdFi_BPS_Staging_Ods.edfi.DisciplineIncident
SELECT * FROM EdFi_BPS_Staging_Ods.edfi.Descriptor WHERE Namespace LIKE '%report%'


SELECT * FROM EdFi_BPS_Staging_Ods.edfi.DisciplineAction

SELECT * FROM EdFi_BPS_Staging_Ods.edfi.DisciplineActionDisciplineIncident

SELECT * FROM EdFi_BPS_Staging_Ods.edfi.StaffEducationOrganizationEmploymentAssociation

select * from EdFi_BPS_Staging_Ods.edfi.DisciplineIncident  where incidenttime is null


*/



