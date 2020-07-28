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
SELECT DISTINCT TOP 10000
     CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),di.[CND_INCIDENT_ID])) AS [_sourceKey],    
     -- dschool.SchoolKey,
	 -- dschool.ShortNameOfInstitution,
	--  dschool.NameOfInstitution,
	  LongitudinalPOC.dbo.Func_GetSchoolYear(di.[CND_INCIDENT_DATE]) AS [SchoolYear],
	  di.CND_INCIDENT_DATE AS [IncidentDate],
	  --cnd.CND_INCIDENT_TIME,
	  --CONVERT(char(12), cnd.CND_INCIDENT_TIME, 108),
	  '00:00:00.0000000' AS IncidentTime,
	  di.[CND_INCIDENT_DESCRIPTION] AS [IncidentDescription],
	  COALESCE(di.CND_INCIDENT_CODE,'Other') as [BehaviorDescriptor_CodeValue],
	  COALESCE(di.CND_INCIDENT_CODE,'Other') as [BehaviorDescriptor_Description],
	  
	  COALESCE(di.[CND_INCIDENT_LOCATION],'Unknown') as [LocationDescriptor_CodeValue],
	  COALESCE(di.[CND_INCIDENT_LOCATION],'Unknown') as [LocationDescriptor_Description],

	  COALESCE(di.[ACT_ACTION_CODE],'Other') as [DisciplineDescriptor_CodeValue],
	  COALESCE(di.[ACT_ACTION_CODE],'Other') as [DisciplineDescriptor_Description],
	  CASE WHEN  COALESCE(di.ACT_ACTION_CODE,'Other') IN ('In-School Suspension)') THEN 1 ELSE 0 END,
	  CASE WHEN  COALESCE(di.ACT_ACTION_CODE,'Other') IN ('Out of School Suspension') THEN 1 ELSE 0 END,
	  
	  'N/A' as ReporterDescriptor_CodeValue,
	  'N/A' as ReporterDescriptor_Description,

	  'N/A' AS [IncidentReporterName],
	  0 AS ReportedToLawEnforcement,
	  0 AS IncidentCost

	 -- @lineageKey AS [LineageKey]
--select distinct *
FROM  [LongitudinalPOC].[Raw_LegacyDW].[DisciplineIncidents] di
	 -- INNER JOIN LongitudinalPOC.dbo.DimSchool dschool ON CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),sch.SKL_SCHOOL_ID))    = dschool._sourceKey
	  INNER JOIN LongitudinalPOC.dbo.DimTime dt ON di.CND_INCIDENT_DATE = dt.SchoolDate
	                                     AND dt.SchoolKey is not null   
									    -- AND dschool.SchoolKey = dt.SchoolKey	
WHERE TRY_CAST(di.CND_INCIDENT_DATE AS DATETIME)  > '2015-09-01'

--CASE WHEN  COALESCE(d_dia.CodeValue,'Other') IN ('In School Suspension','In-School Suspension)') THEN 1 ELSE 0 END,
--CASE WHEN  COALESCE(d_dia.CodeValue,'Other') IN ('Out of School Suspension','Out-Of-School Suspension)') THEN 1 ELSE 0 END,


--SELECT * FROM [BPSDATA-03].[ExtractAspen].[dbo].[SCHOOL]
--select * from LongitudinalPOC.[dbo].[DimDisciplineIncident]

--updatng the lineage table
UPDATE LongitudinalPOC.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;

