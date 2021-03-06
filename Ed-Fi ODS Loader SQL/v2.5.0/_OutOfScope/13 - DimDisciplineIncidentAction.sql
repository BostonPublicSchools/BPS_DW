DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM BPS_DW.[dbo].[Lineage] WHERE TableName= 'dbo.DimDisciplineIncidentAction')
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
	('dbo.DimDisciplineIncidentAction', 
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
	 WHERE TableName= 'dbo.DimDisciplineIncidentAction'
END 



INSERT INTO BPS_DW.[dbo].DimDisciplineIncidentAction
           ([_sourceKey]
           ,[DisciplineDescriptor_CodeValue]
           ,[DisciplineDescriptor_Description]
		   ,DisciplineDescriptor_ISS_Indicator
		   ,DisciplineDescriptor_OSS_Indicator
           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])

SELECT DISTINCT 
      'Ed-Fi|' + Convert(NVARCHAR(MAX),d.DescriptorId) AS [_sourceKey]	,
	  ISNULL(d.CodeValue,'Other') as [DisciplineDescriptor_CodeValue],
	  ISNULL(d.Description,'Other') as [DisciplineDescriptor_Description],
	  CASE WHEN  ISNULL(d.CodeValue,'Other') IN ('In School Suspension','In-School Suspension)') THEN 1 ELSE 0 END,
	  CASE WHEN  ISNULL(d.CodeValue,'Other') IN ('Out of School Suspension','Out-Of-School Suspension)') THEN 1 ELSE 0 END,
	  GETDATE() AS ValidFrom,
	  '12/31/9999' AS ValidTo,
	  1  AS IsCurrent,
	  @lineageKey AS [LineageKey]
--select *  
FROM [EdFi_BPS_Staging_Ods].edfi.Descriptor d
WHERE d.Namespace IN ('http://ed-fi.org/Descriptor/DisciplineDescriptor.xml','http://ed-fi.org/Descriptor/Follett/Aspen/DisciplineDescriptor.xml');

--select * from BPS_DW.[dbo].[DimDisciplineIncidentAction]

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


*/



