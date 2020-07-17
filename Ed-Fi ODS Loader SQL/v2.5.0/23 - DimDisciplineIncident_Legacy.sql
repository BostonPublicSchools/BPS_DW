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
     CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),cnd.CND_INCIDENT_ID)) AS [_sourceKey],    
     -- dschool.SchoolKey,
	 -- dschool.ShortNameOfInstitution,
	--  dschool.NameOfInstitution,
	  LongitudinalPOC.dbo.Func_GetSchoolYear(cnd.CND_INCIDENT_DATE) AS [SchoolYear],
	  cnd.CND_INCIDENT_DATE AS [IncidentDate],
	  --cnd.CND_INCIDENT_TIME,
	  --CONVERT(char(12), cnd.CND_INCIDENT_TIME, 108),
	  COALESCE(CONVERT(char(12), cnd.CND_INCIDENT_TIME, 108),'00:00:00.0000000') AS IncidentTime,
	  cnd.CND_INCIDENT_DESCRIPTION AS [IncidentDescription],
	  COALESCE(cnd.CND_INCIDENT_CODE,'Other') as [BehaviorDescriptor_CodeValue],
	  COALESCE(cnd.CND_INCIDENT_CODE,'Other') as [BehaviorDescriptor_Description],
	  
	  COALESCE(cnd.CND_INCIDENT_LOCATION,'Unknown') as [LocationDescriptor_CodeValue],
	  COALESCE(cnd.CND_INCIDENT_LOCATION,'Unknown') as [LocationDescriptor_Description],

	  COALESCE(act.ACT_ACTION_CODE,'Other') as [DisciplineDescriptor_CodeValue],
	  COALESCE(act.ACT_ACTION_CODE,'Other') as [DisciplineDescriptor_Description],
	  CASE WHEN  COALESCE(act.ACT_ACTION_CODE,'Other') IN ('Susp in school','In-School Suspension)') THEN 1 ELSE 0 END,
	  CASE WHEN  COALESCE(act.ACT_ACTION_CODE,'Other') IN ('Suspension Out','Susp out no svc','Susp out with svc','Susp out >10 cumul','Susp out >10 consec') THEN 1 ELSE 0 END,
	  
	  'N/A' as ReporterDescriptor_CodeValue,
	  'N/A' as ReporterDescriptor_Description,

	  'N/A' AS [IncidentReporterName],
	  0 AS ReportedToLawEnforcement,
	  0 AS IncidentCost

	 -- @lineageKey AS [LineageKey]
--select distinct *
FROM  [BPSDATA-03].[ExtractAspen].[dbo].[STUDENT_CONDUCT_INCIDENT] cnd
      JOIN [BPSDATA-03].[ExtractAspen].[dbo].[STUDENT_CONDUCT_ACTION] act On cnd.CND_OID = act.ACT_CND_OID
	  JOIN [BPSDATA-03].[ExtractAspen].[dbo].[SCHOOL] sch On cnd.CND_SKL_OID = sch.SKL_OID

	  INNER JOIN LongitudinalPOC.dbo.DimSchool dschool ON CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),sch.SKL_SCHOOL_ID))    = dschool._sourceKey
	  INNER JOIN LongitudinalPOC.dbo.DimTime dt ON cnd.CND_INCIDENT_DATE = dt.SchoolDate
	                                     AND dt.SchoolKey is not null   
									     AND dschool.SchoolKey = dt.SchoolKey	
WHERE TRY_CAST(cnd.CND_INCIDENT_DATE AS DATETIME)  > '2015-09-01'


--SELECT * FROM [BPSDATA-03].[ExtractAspen].[dbo].[SCHOOL]
--select * from LongitudinalPOC.[dbo].[DimDisciplineIncident]

--updatng the lineage table
UPDATE LongitudinalPOC.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;

