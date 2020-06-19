DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM BPS_DW.[dbo].[Lineage] WHERE TableName= 'dbo.FactStudentAttendanceByDay')
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
	 FROM BPS_DW.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.FactStudentAttendanceByDay'
END 

truncate table BPS_DW.[dbo].[FactStudentAttendanceByDay]


INSERT INTO BPS_DW.[dbo].[FactStudentAttendanceByDay]
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
	  ISNULL(ssae.AttendanceEventReason,'N/A') ,
	  @lineageKey AS [LineageKey]
--select *  
FROM [EdFi_BPS_Staging_Ods].edfi.StudentSchoolAssociation ssa 
    INNER JOIN [EdFi_BPS_Staging_Ods].edfi.CalendarDate cda on ssa.SchoolId = cda.SchoolId 														   
	INNER JOIN [EdFi_BPS_Staging_Ods].edfi.CalendarDateCalendarEvent cdce on cda.Date=cdce.Date 
																		 and cda.SchoolId=cdce.SchoolId
	INNER JOIN [EdFi_BPS_Staging_Ods].edfi.Descriptor d_cdce on cdce.CalendarEventDescriptorId = d_cdce.DescriptorId
														  and d_cdce.CodeValue='Instructional day' -- ONLY Instructional days
	
	LEFT JOIN [EdFi_BPS_Staging_Ods].edfi.StudentSchoolAttendanceEvent ssae on ssa.StudentUSI = ssae.StudentUSI
	                                                                        and ssa.SchoolId = ssae.SchoolId 
																			and cda.Date = ssae.EventDate
																	
	--joining DW tables
	INNER JOIN BPS_DW.dbo.DimStudent ds  ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ssa.StudentUSI)   = ds._sourceKey
	INNER JOIN BPS_DW.dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ssa.SchoolId)   = dschool._sourceKey
	INNER JOIN BPS_DW.dbo.DimTime dt ON cdce.Date = dt.SchoolDate
	                                and dt.SchoolKey is not null   
									and dschool.SchoolKey = dt.SchoolKey
    INNER JOIN BPS_DW.[dbo].DimAttendanceEventCategory daec ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ssae.AttendanceEventCategoryDescriptorId)  = daec._sourceKey


WHERE  cdce.Date >= ssa.EntryDate 
   and cdce.Date <= GETDATE()
   and (
         (ssa.ExitWithdrawDate is null) 
	      OR
         (ssa.ExitWithdrawDate is not null and cdce.Date<=ssa.ExitWithdrawDate) 
	   )
	and ssa.SchoolYear IN (2019, 2020)
	--AND ssa.SchoolId in (2360) -- Lilla G. Frederick Pilot Middle School
--order by s.StudentUSI;

--select * from BPS_DW.[dbo].[FactStudentAttendanceByDay]

--updatng the lineage table
UPDATE BPS_DW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;



/*

SELECT * FROM dbo.DimSchool WHERE IsCurrent = 1
select * from [EdFi_BPS_Staging_Ods].edfi.School WHERE SchoolId = 2360
select * from [EdFi_BPS_Staging_Ods].edfi.EducationOrganization WHERE EducationOrganizationId = 2360

select *
select * from [EdFi_BPS_Staging_Ods].edfi.StudentSchoolAttendanceEvent
select * from [EdFi_BPS_Staging_Ods].edfi.StudentSectionAttendanceEvent
select * from [EdFi_BPS_Staging_Ods].edfi.StudentSchoolAssociation


select * from [EdFi_BPS_Staging_Ods].edfi.StaffSectionAssociation

select * from [EdFi_BPS_Staging_Ods].edfi.Descriptor where Namespace like '%attendance%'


SELECT TOP 5 * FROM [EdFi_BPS_Staging_Ods].edfi.StudentSectionAssociation
SELECT TOP 5 * FROM [EdFi_BPS_Staging_Ods].edfi.StudentSchoolAssociation
SELECT TOP 5 * FROM [EdFi_BPS_Staging_Ods].edfi.Section

SELECT TOP 5 * FROM [EdFi_BPS_Staging_Ods].edfi.School

SELECT * FROM BPS_DW.dbo.DimTime

SELECT TOP 5 * FROM [EdFi_BPS_Staging_Ods].edfi.SectionAttendanceTakenEvent

SELECT TOP 5 * FROM [EdFi_BPS_Staging_Ods].edfi.StudentSectionAttendanceEvent

*/



