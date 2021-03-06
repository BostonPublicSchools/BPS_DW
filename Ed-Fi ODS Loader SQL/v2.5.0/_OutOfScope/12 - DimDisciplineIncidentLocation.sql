DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM LongitudinalPOC.[dbo].[Lineage] WHERE TableName= 'dbo.CREATE TABLE dbo.DimDisciplineIncidentLocation')
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
	('dbo.CREATE TABLE dbo.DimDisciplineIncidentLocation', 
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
	 WHERE TableName= 'dbo.CREATE TABLE dbo.DimDisciplineIncidentLocation'
END 






INSERT INTO LongitudinalPOC.[dbo].DimDisciplineIncidentLocation
           ([_sourceKey]
           ,[LocationDescriptor_CodeValue]
           ,[LocationDescriptor_Description]
           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])

SELECT DISTINCT 
      'Ed-Fi|' + Convert(NVARCHAR(MAX),IncidentLocationTypeId) AS [_sourceKey]	,
	  ISNULL(CodeValue,'Unknown') as [LocationDescriptor_CodeValue],
	  ISNULL(Description,'Unknown') as [LocationDescriptor_Description],
	  GETDATE() AS ValidFrom,
	  '12/31/9999' AS ValidTo,
	  1  AS IsCurrent,
	  @lineageKey AS [LineageKey]
--select *  
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.IncidentLocationType 

--SELECT * from LongitudinalPOC.[dbo].[DimDisciplineIncidentLocation]

--updatng the lineage table
UPDATE LongitudinalPOC.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;


/*
--discilpline type
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncidentBehavior
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor WHERE Namespace LIKE '%behavior%'


--location
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncident
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.IncidentLocationType


--action
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineActionDiscipline
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor WHERE Namespace LIKE '%discipline%'


--reporter type
SELECT IncidentDate FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncident
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor WHERE Namespace LIKE '%report%'


SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineAction

SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineActionDisciplineIncident

SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffEducationOrganizationEmploymentAssociation


*/



