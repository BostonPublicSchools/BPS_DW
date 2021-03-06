DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM EdFiDW.[dbo].[Lineage] WHERE TableName= 'dbo.DimAttendanceEventCategory')
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
	('dbo.DimAttendanceEventCategory', 
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
	 WHERE TableName= 'dbo.DimAttendanceEventCategory'
END 



INSERT INTO EdFiDW.[dbo].DimAttendanceEventCategory
           ([_sourceKey]
           ,[AttendanceEventCategoryDescriptor_CodeValue]
           ,[AttendanceEventCategoryDescriptor_Description]
           ,[InAttendance_Indicator]
           ,[UnexcusedAbsence_Indicator]
           ,[ExcusedAbsence_Indicator]
           ,[Tardy_Indicator]
           ,[EarlyDeparture_Indicator]
           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])

SELECT DISTINCT 
      'Ed-Fi|' + Convert(NVARCHAR(MAX),d.DescriptorId) AS [_sourceKey]	,
	  COALESCE(d.CodeValue,'In Attendance') as AttendanceEventCategoryDescriptor_CodeValue,
	  COALESCE(d.CodeValue,'In Attendance') as AttendanceEventCategoryDescriptor_Description,
	  case when COALESCE(d.CodeValue,'In Attendance') in ('In Attendance','Tardy','Early departure') then 1 else 0 end as [InAttendance_Indicator], -- not used
	  case when COALESCE(d.CodeValue,'In Attendance') in ('Unexcused Absence') then 1 else 0 end as [UnexcusedAbsence_Indicator],
	  case when COALESCE(d.CodeValue,'In Attendance') in ('Excused Absence') then 1 else 0 end as [ExcusedAbsence_Indicator],
	  case when COALESCE(d.CodeValue,'In Attendance') in ('Tardy') then 1 else 0 end as [Tardy_Indicator],	   
	  case when COALESCE(d.CodeValue,'In Attendance') in ('Early departure') then 1 else 0 end as [EarlyDeparture_Indicator],	  
	  GETDATE() AS ValidFrom,
	  '12/31/9999' AS ValidTo,
	  1  AS IsCurrent,
	  @lineageKey AS [LineageKey]
--select *  
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d
WHERE d.Namespace IN ('http://ed-fi.org/Descriptor/AttendanceEventCategoryDescriptor.xml','http://ed-fi.org/Descriptor/Follett/Aspen/AttendanceEventCategoryDescriptor.xml');

--select * from EdFiDW.[dbo].[DimAttendanceEventCategory]

--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;


/*
select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAttendanceEvent
select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSectionAttendanceEvent
select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation


select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffSectionAssociation

select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor where Namespace like '%attendance%'


SELECT TOP 5 * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSectionAssociation
SELECT TOP 5 * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation
SELECT TOP 5 * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Section

SELECT TOP 5 * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School

SELECT * FROM EdFiDW.dbo.DimTime

SELECT TOP 5 * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SectionAttendanceTakenEvent

SELECT TOP 5 * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSectionAttendanceEvent
*/



