DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM LongitudinalPOC.[dbo].[Lineage] WHERE TableName= 'dbo.DimDisciplineIncidentReporterType')
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
	('dbo.DimDisciplineIncidentReporterType', 
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
	 WHERE TableName= 'dbo.DimDisciplineIncidentReporterType'
END 






INSERT INTO LongitudinalPOC.[dbo].DimDisciplineIncidentReporterType
           ([_sourceKey]
           ,ReporterDescriptor_CodeValue
           ,ReporterDescriptor_Description
           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])

SELECT DISTINCT 
      'Ed-Fi|' + Convert(NVARCHAR(MAX),d.DescriptorId) AS [_sourceKey]	,
	  ISNULL(d.CodeValue,'Other') as ReporterDescriptor_CodeValue,
	  ISNULL(d.Description,'Other') as ReporterDescriptor_Description,
	  GETDATE() AS ValidFrom,
	  '12/31/9999' AS ValidTo,
	  1  AS IsCurrent,
	  @lineageKey AS [LineageKey]
--select *  
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d
WHERE d.Namespace IN ('http://ed-fi.org/Descriptor/ReporterDescriptionDescriptor.xml');

--select * from LongitudinalPOC.[dbo].[DimDisciplineIncidentReporterType]

--updatng the lineage table
UPDATE LongitudinalPOC.[dbo].[Lineage]
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



