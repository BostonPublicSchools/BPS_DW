DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM EdFiDW.[dbo].[Lineage] WHERE TableName= 'dbo.FactStudentAttendanceByDay')
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
	 FROM EdFiDW.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.FactStudentAttendanceByDay'
END 

truncate table EdFiDW.[dbo].[FactStudentAttendanceByDay]

--;WITH AttedanceEvents AS
--(
	SELECT DISTINCT StudentUSI, 
					SchoolId, 
					SchoolYear, 
					EventDate,
					AttendanceEventCategoryDescriptorId,
					CASE WHEN COALESCE(AttendanceEventReason,'') = '' or LTRIM(RTRIM(AttendanceEventReason)) = '' THEN 'N/A'
						 ELSE AttendanceEventReason
					END AS AttendanceEventReason , 
					ROW_NUMBER() OVER (PARTITION BY StudentUSI, 
													SchoolId, 
													SchoolYear, 
													EventDate
									   ORDER BY AttendanceEventReason DESC) AS RowId INTO #AttedanceEvents
	FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAttendanceEvent
	WHERE SchoolYear IN (2019,2020);
	--AND StudentUSI = 64757
	--AND EventDate = '2019-11-04'

--)
INSERT INTO EdFiDW.[dbo].[FactStudentAttendanceByDay]
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
	  ISNULL(ssae.AttendanceEventReason,'N/A') AS  AttendanceEventReason,
	  @lineageKey AS [LineageKey]
--select *  
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
    INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CalendarDate cda on ssa.SchoolId = cda.SchoolId 														   
	INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CalendarDateCalendarEvent cdce on cda.Date=cdce.Date 
																		 and cda.SchoolId=cdce.SchoolId
	INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d_cdce on cdce.CalendarEventDescriptorId = d_cdce.DescriptorId
														  and d_cdce.CodeValue='Instructional day' -- ONLY Instructional days
	
	LEFT JOIN #AttedanceEvents ssae on ssa.StudentUSI = ssae.StudentUSI
	                                               AND ssa.SchoolId = ssae.SchoolId 
												   AND cda.Date = ssae.EventDate
												   AND ssae.RowId= 1			
	--joining DW tables
	INNER JOIN EdFiDW.dbo.DimStudent ds  ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ssa.StudentUSI)   = ds._sourceKey
	INNER JOIN EdFiDW.dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ssa.SchoolId)   = dschool._sourceKey
	INNER JOIN EdFiDW.dbo.DimTime dt ON cdce.Date = dt.SchoolDate
	                                and dt.SchoolKey is not null   
									and dschool.SchoolKey = dt.SchoolKey
    LEFT JOIN EdFiDW.[dbo].DimAttendanceEventCategory daec ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ssae.AttendanceEventCategoryDescriptorId)  = daec._sourceKey
	                                                       OR daec.AttendanceEventCategoryDescriptor_CodeValue = 'In Attendance'
 
WHERE  cdce.Date >= ssa.EntryDate 
   and cdce.Date <= GETDATE()
   and (
         (ssa.ExitWithdrawDate is null) 
	      OR
         (ssa.ExitWithdrawDate is not null and cdce.Date<=ssa.ExitWithdrawDate) 
	   )
	and ssa.SchoolYear IN (2019,2020)
	--AND ssa.StudentUSI = 64757
	--AND ssa.SchoolId in (2360) -- Lilla G. Frederick Pilot Middle School
--order by s.StudentUSI;
DROP TABLE #AttedanceEvents;
--select top 1 * from EdFiDW.[dbo].[FactStudentAttendanceByDay]

--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;



/*

--17907, 1494, 663, 3
56749, 37781, 168, 2
--SELECT * FROM EdFiDW.dbo.DimStudent WHERE StudentKey = 17907
--SELECT * FROM EdFiDW.dbo.DimStudent WHERE StudentKey = 37781
--SELECT * FROM EdFiDW.dbo.DimTime WHERE TimeKey = 1494
--SELECT * FROM EdFiDW.dbo.DimTime WHERE TimeKey = 37781
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

SELECT * FROM EdFiDW.dbo.DimTime

SELECT TOP 5 * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SectionAttendanceTakenEvent

SELECT TOP 5 * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSectionAttendanceEvent

select * from EdFiDW.[dbo].[FactStudentAttendanceByDay]

*/



