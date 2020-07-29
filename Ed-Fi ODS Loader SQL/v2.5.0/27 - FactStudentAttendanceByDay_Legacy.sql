DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM LongitudinalPOC.[dbo].[Lineage] WHERE TableName= 'dbo.FactStudentAttendanceByDay')
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
	('dbo.FactStudentAttendanceByDay', 
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
	 WHERE TableName= 'dbo.FactStudentAttendanceByDay'
END 

--truncate table LongitudinalPOC.[dbo].[FactStudentAttendanceByDay]

INSERT INTO LongitudinalPOC.[dbo].[FactStudentAttendanceByDay]
           ([StudentKey]
           ,[TimeKey]
           ,[SchoolKey]
           ,[AttendanceEventCategoryKey]
           ,[AttendanceEventReason]
           ,[LineageKey])

SELECT DISTINCT 
      ds.StudentKey,
      dt.TimeKey,	  
	  dschool.SchoolKey,      
	  daec.AttendanceEventCategoryKey,
	  'N/A' AS  AttendanceEventReason,
	  @lineageKey AS [LineageKey]
--select *  
FROM [BPSDW].[dbo].[Attendance] a	
	--joining DW tables
	INNER JOIN LongitudinalPOC.dbo.DimStudent ds  ON CONCAT_WS('|', 'LegacyDW', Convert(NVARCHAR(MAX),a.[StudentNo]))   = ds._sourceKey
	INNER JOIN LongitudinalPOC.dbo.DimSchool dschool ON CONCAT_WS('|', 'LegacyDW', Convert(NVARCHAR(MAX),a.Sch))   = dschool._sourceKey
	INNER JOIN LongitudinalPOC.dbo.DimTime dt ON a.[Date] = dt.SchoolDate
	                                and dt.SchoolKey is not null   
									and dschool.SchoolKey = dt.SchoolKey
    LEFT JOIN LongitudinalPOC.[dbo].DimAttendanceEventCategory daec ON CASE 
	                                                                        WHEN a.AttendanceCodeDesc IN ('Absent') THEN 'Unexcused Absence'
	                                                                        WHEN a.AttendanceCodeDesc IN ('Absent, Bus Strike','Bus / Transportation','Excused Absent','Dismissed','In School, Suspended','Suspended') THEN 'Excused Absence'
																			WHEN a.AttendanceCodeDesc IN ('Early Dismissal')  THEN 'Early departure'
																			WHEN a.AttendanceCodeDesc = 'No Contact'  THEN 'No Contact'
																			WHEN CHARINDEX('Tardy',a.AttendanceCodeDesc,1) > 0 THEN 'Tardy'

																	        ELSE 'In Attendance' 
	                                                                   
                                                                       END = daec.AttendanceEventCategoryDescriptor_CodeValue
 
WHERE  a.[Date] >= '2015-07-01'

--Absent   - Unexcused Absence
--Absent, Bus Strike  - Excused Absence
--Bus / Transportation - Excused Absence
--Constructively Present ? In Attendance ?
--Dismissed ? Excused Absence
--Early Dismissal - Early departure
--Excused Absent - Excused Absence
--In School, Suspended -  Excused Absence 
--No Contact - No Contact
--Present - In Attendance
--Suspended - Excused Absence
--Tardy - Tardy
--Tardy, Bus Strike  - Tardy
--Tardy, Greater Than Half Day - Tardy
--Tardy/Early Dismissal Greater Than Half Day - Tardy
--Tardy/Early Dismissal Less Than Half Day - Tardy
--Tardy/Excused Less Than Half DAY - Tardy

--EDFI
---------------------
--Early departure
--Excused Absence
--In Attendance
--No Contact
--Tardy
--Unexcused Absence



--select * from LongitudinalPOC.[dbo].[FactStudentAttendanceByDay]

--updatng the lineage table
UPDATE LongitudinalPOC.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;



/*


SELECT DISTINCT AttendanceEventCategoryDescriptor_CodeValue FROM LongitudinalPOC.[dbo].DimAttendanceEventCategory
SELECT * FROM LongitudinalPOC.[dbo].DimAttendanceEventCategory
SELECT DISTINCT TOP 100 *  FROM [BPSDW].[dbo].[Attendance] where date >= '2015-07-01' order by date ASC
  

--17907, 1494, 663, 3
56749, 37781, 168, 2
--SELECT * FROM LongitudinalPOC.dbo.DimStudent WHERE StudentKey = 17907
--SELECT * FROM LongitudinalPOC.dbo.DimStudent WHERE StudentKey = 37781
--SELECT * FROM LongitudinalPOC.dbo.DimTime WHERE TimeKey = 1494
--SELECT * FROM LongitudinalPOC.dbo.DimTime WHERE TimeKey = 37781
--SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAttendanceEvent WHERE StudentUSI = 20170 AND EventDate = '2019-10-01'
--SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAttendanceEvent WHERE StudentUSI = 64757 AND EventDate = '2019-11-04'

--SELECT * FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor WHERE DescriptorId IN (742,753)
--SELECT * FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor WHERE DescriptorId IN (742,753)
--select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Session where TermDescriptorId IN (742,753) and SchoolId = 4410
SELECT * FROM dbo.DimSchool WHERE IsCurrent = 1
select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School WHERE SchoolId = 2360
select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization WHERE EducationOrganizationId = 2360

select *
select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAttendanceEvent
select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSectionAttendanceEvent
select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation


select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffSectionAssociation

select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor where Namespace like '%attendance%'


SELECT TOP 5 * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSectionAssociation
SELECT TOP 5 * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation
SELECT TOP 5 * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Section

SELECT TOP 5 * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School

SELECT * FROM LongitudinalPOC.dbo.DimTime

SELECT TOP 5 * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SectionAttendanceTakenEvent

SELECT TOP 5 * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSectionAttendanceEvent

select * from LongitudinalPOC.[dbo].[FactStudentAttendanceByDay]

*/



