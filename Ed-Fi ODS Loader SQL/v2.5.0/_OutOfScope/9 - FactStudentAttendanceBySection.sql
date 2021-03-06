DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM BPS_DW.[dbo].[Lineage] WHERE TableName= 'dbo.FactStudentAttendanceBySection')
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
	('dbo.FactStudentAttendanceBySection', 
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
	 WHERE TableName= 'dbo.FactStudentAttendanceBySection'
END 

truncate table BPS_DW.[dbo].[FactStudentAttendanceBySection]

/*
CREATE NONCLUSTERED INDEX [IX_DimeTimne_School-SchoolKey_Including-TimeKey]
ON [dbo].[DimTime] ([SchoolDate],[SchoolKey])
INCLUDE ([TimeKey])
GO

CREATE NONCLUSTERED INDEX [IX_DimStudent_sourceKey_Including-StudentKey]
ON [dbo].[DimStudent] ([_sourceKey])
INCLUDE ([StudentKey])
GO

CREATE NONCLUSTERED INDEX [IX_DimSection_sourceKey_Including-SectionKey]
ON [dbo].[DimSection] ([_sourceKey])
INCLUDE ([SectionKey])
GO

CREATE NONCLUSTERED INDEX [IX_DimStaff_sourceKey_Including-StaffKey]
ON [dbo].[DimStaff] ([_sourceKey])
INCLUDE ([StaffKey])
GO

*/


INSERT INTO BPS_DW.[dbo].[FactStudentAttendanceBySection]
           ([StudentKey]
           ,[TimeKey]
           ,[SectionKey]
           ,[StaffKey]
           ,[SchoolKey]
           ,[AttendanceEventCategoryKey]
           ,[AttendanceEventReason]
           ,[LineageKey])

SELECT 
      ds.StudentKey,
      dt.TimeKey,
	  dsection.SectionKey,
	  dstaff.StaffKey,
	  dschool.SchoolKey,      
	  daec.AttendanceEventCategoryKey,
	  ssae.AttendanceEventReason ,
	  @lineageKey AS [LineageKey]
--select *  
FROM [EdFi_BPS_Staging_Ods].edfi.Student s    
	INNER JOIN [EdFi_BPS_Staging_Ods].edfi.StudentSchoolAssociation ssa on s.StudentUSI = ssa.StudentUSI 	

	INNER JOIN [EdFi_BPS_Staging_Ods].edfi.StudentSectionAssociation s_sect_a on s.StudentUSI = s_sect_a.StudentUSI
	                                                                         and ssa.SchoolId =  s_sect_a.SchoolId
	INNER JOIN [EdFi_BPS_Staging_Ods].edfi.StaffSectionAssociation staff_sect_a on  s_sect_a.UniqueSectionCode = staff_sect_a.UniqueSectionCode													               
	INNER JOIN [EdFi_BPS_Staging_Ods].edfi.Staff staff on staff_sect_a.StaffUSI = staff.StaffUSI
	INNER JOIN [EdFi_BPS_Staging_Ods].edfi.CalendarDate cda on s_sect_a.SchoolId = cda.SchoolId 														   
	INNER JOIN [EdFi_BPS_Staging_Ods].edfi.CalendarDateCalendarEvent cdce on cda.Date=cdce.Date 
																		 and cda.SchoolId=cdce.SchoolId
	INNER JOIN [EdFi_BPS_Staging_Ods].edfi.Descriptor d_cdce on cdce.CalendarEventDescriptorId = d_cdce.DescriptorId
														  and d_cdce.CodeValue='Instructional day' -- ONLY Instructional days
	
	LEFT JOIN [EdFi_BPS_Staging_Ods].edfi.StudentSectionAttendanceEvent ssae on s.StudentUSI = ssae.StudentUSI
	                                                                        and ssa.SchoolId = ssae.SchoolId 
																			and cda.Date = ssae.EventDate
																			and  s_sect_a.UniqueSectionCode = ssae.UniqueSectionCode
	--joining DW tables
	INNER JOIN BPS_DW.dbo.DimStudent ds  ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),s.StudentUSI)   = ds._sourceKey
	INNER JOIN BPS_DW.dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),s_sect_a.SchoolId)   = dschool._sourceKey
	INNER JOIN BPS_DW.dbo.DimSection dsection ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),s_sect_a.UniqueSectionCode)   = dsection._sourceKey
	INNER JOIN BPS_DW.dbo.DimStaff dstaff ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),staff.StaffUSI)   = dstaff._sourceKey
	INNER JOIN BPS_DW.dbo.DimTime dt ON cdce.Date = dt.SchoolDate
	                                and dt.SchoolKey is not null   
									and dschool.SchoolKey = dt.SchoolKey
    INNER JOIN BPS_DW.[dbo].DimAttendanceEventCategory daec ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ssae.AttendanceEventCategoryDescriptorId)  = daec._sourceKey
   [edfi].[StaffSectionAssociation]
	SELECT * FROM [EdFi_BPS_Staging_Ods].edfi.StaffSectionAssociation 
WHERE  cdce.Date >= ssa.EntryDate 
   and cdce.Date <= GETDATE()
   and (
         (ssa.ExitWithdrawDate is null) 
	      OR
         (ssa.ExitWithdrawDate is not null and cdce.Date<=ssa.ExitWithdrawDate) 
	   )
	and ssa.SchoolYear IN (2019, 2020)
	AND ssa.SchoolId in (2360) -- Lilla G. Frederick Pilot Middle School
--order by s.StudentUSI;

--select * from BPS_DW.[dbo].[FactStudentAttendanceBySection]

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



