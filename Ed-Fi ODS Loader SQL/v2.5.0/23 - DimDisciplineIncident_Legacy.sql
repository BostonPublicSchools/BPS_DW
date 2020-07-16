DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM LongitudinalPOC.[dbo].[Lineage] WHERE TableName= 'dbo.DimDisciplineIncident')
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
	 FROM LongitudinalPOC.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.DimDisciplineIncident'
END 


DELETE FROM LongitudinalPOC.[dbo].DimDisciplineIncident
WHERE SchoolYear = LongitudinalPOC.dbo.Func_GetSchoolYear(GETDATE())
         

INSERT INTO LongitudinalPOC.[dbo].DimDisciplineIncident
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
     CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),cnd.CND_INCIDENT_ID)) AS [_sourceKey],    
     -- dschool.SchoolKey,
	 -- dschool.ShortNameOfInstitution,
	--  dschool.NameOfInstitution,
	  LongitudinalPOC.dbo.Func_GetSchoolYear(cnd.CND_INCIDENT_DATE) AS [SchoolYear],
	  cnd.CND_INCIDENT_DATE AS [IncidentDate],
	  cnd.CND_INCIDENT_TIME,
	  CONVERT(char(5), cnd.CND_INCIDENT_TIME, 108),
	  --ISNULL(cnd.CND_INCIDENT_TIME,'00:00:00.0000000') AS IncidentTime,

	  ISNULL(cnd.CND_INCIDENT_CODE,'Other') as [BehaviorDescriptor_CodeValue],
	  ISNULL(cnd.CND_INCIDENT_CODE,'Other') as [BehaviorDescriptor_Description],
	  
	  ISNULL(cnd.CND_INCIDENT_LOCATION,'Unknown') as [LocationDescriptor_CodeValue],
	  ISNULL(cnd.CND_INCIDENT_LOCATION,'Unknown') as [LocationDescriptor_Description],

	  ISNULL(act.ACT_ACTION_CODE,'Other') as [DisciplineDescriptor_CodeValue],
	  ISNULL(act.ACT_ACTION_CODE,'Other') as [DisciplineDescriptor_Description],
	  CASE WHEN  ISNULL(act.ACT_ACTION_CODE,'Other') IN ('Susp in school','In-School Suspension)') THEN 1 ELSE 0 END,
	  CASE WHEN  ISNULL(act.ACT_ACTION_CODE,'Other') IN ('Suspension Out','Susp out no svc','Susp out with svc','Susp out >10 cumul','Susp out >10 consec') THEN 1 ELSE 0 END,
	  
	  'N/A' as ReporterDescriptor_CodeValue,
	  'N/A' as ReporterDescriptor_Description,

	  'N/A' AS [IncidentReporterName],
	  0 AS ReportedToLawEnforcement,
	  0 AS IncidentCost

	 -- @lineageKey AS [LineageKey]
--select distinct *
FROM  [BPSDATA-03].[ExtractAspen].[dbo].[STUDENT_CONDUCT_INCIDENT] cnd
      JOIN [BPSDATA-03].[ExtractAspen].[dbo].[STUDENT_CONDUCT_ACTION] act On cnd.CND_OID = act.ACT_CND_OID

	  --INNER JOIN LongitudinalPOC.dbo.DimSchool dschool ON CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),cnd.SchoolId))    = dschool._sourceKey
	  --INNER JOIN LongitudinalPOC.dbo.DimTime dt ON cnd.CND_INCIDENT_DATE = dt.SchoolDate
	                                    -- AND dt.SchoolKey is not null   
									     --AND dschool.SchoolKey = dt.SchoolKey	
WHERE TRY_CAST(cnd.CND_INCIDENT_DATE AS DATETIME)  > '2015-09-01'

--select * from LongitudinalPOC.[dbo].[DimDisciplineIncident]

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

select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncident  where incidenttime is null


*/



