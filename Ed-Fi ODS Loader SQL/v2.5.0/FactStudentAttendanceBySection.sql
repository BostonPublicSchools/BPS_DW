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

INSERT INTO BPS_DW.[dbo].[FactStudentAttendanceBySection]
           ([StudentKey]
           ,[TimeKey]
           ,[SectionKey]
           ,[StaffKey]
		   ,[SchoolKey]
           ,AttendanceEventCategoryDescriptor_CodeValue
		   ,AttendanceEventCategoryDescriptor_Description
		   ,AttendanceEventReason
           ,[InAttendance_Indicator]
           ,[UnexcusedAbsence_Indicator]
           ,[ExcusedAbsence_Indicator]
           ,[Tardy_Indicator]		   		  
           ,[EarlyDeparture_Indicator]
		   --,[UnexcusedTardy_Indicator]
           --,[ExcusedTardy_Indicator]
		   --,[NoContact_Indicator]		   
           --,[ADA_Indicator]
           ,[LineageKey])

SELECT count(*) 
       --ds.StudentKey,
       --dt.TimeKey,
	   --dsection.SectionKey,
	   --dstaff.StaffKey,
	   --dschool.SchoolKey,
       --
	   --ISNULL(d_ssae.CodeValue,'In Attendance') as AttendanceEventCategoryDescriptor_CodeValue,
	   --ISNULL(d_ssae.CodeValue,'In Attendance') as AttendanceEventCategoryDescriptor_Description,
	   --ssae.AttendanceEventReason,
	   --case when ISNULL(d_ssae.CodeValue,'In Attendance') in ('In Attendance','Tardy','Early departure') then 1 else 0 end as [InAttendance_Indicator], -- not used
	   --case when ISNULL(d_ssae.CodeValue,'In Attendance') in ('Unexcused Absence') then 1 else 0 end as [UnexcusedAbsence_Indicator],
	   --case when ISNULL(d_ssae.CodeValue,'In Attendance') in ('Excused Absence') then 1 else 0 end as [ExcusedAbsence_Indicator],
	   --case when ISNULL(d_ssae.CodeValue,'In Attendance') in ('Tardy') then 1 else 0 end as [Tardy_Indicator],	   
	   --case when ISNULL(d_ssae.CodeValue,'In Attendance') in ('Early departure') then 1 else 0 end as [EarlyDeparture_Indicator]
	   --case when d_ssae.CodeValue in ('Tardy') then 1 else 0 end as [UnexcusedTardy_Indicator], -- assuming all tardies are un excused
	   --0 as [ExcusedTardy_Indicator],
	   --case when d_ssae.CodeValue not in ('No Contact') then 1 else 0 end as [NoContact_Indicator],
	  -- @lineageKey AS [LineageKey]
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
    LEFT JOIN [EdFi_BPS_Staging_Ods].edfi.Descriptor d_ssae on ssae.AttendanceEventCategoryDescriptorId = d_ssae.DescriptorId
	                                                      
	

	--joining DW tables
	INNER JOIN BPS_DW.dbo.DimStudent ds  ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),s.StudentUSI)   = ds._sourceKey
	INNER JOIN BPS_DW.dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),s_sect_a.SchoolId)   = dschool._sourceKey
	INNER JOIN BPS_DW.dbo.DimSection dsection ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),s_sect_a.UniqueSectionCode)   = dsection._sourceKey
	INNER JOIN BPS_DW.dbo.DimStaff dstaff ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),staff.StaffUSI)   = dstaff._sourceKey
	INNER JOIN BPS_DW.dbo.DimTime dt ON cdce.Date = dt.SchoolDate
	                                and dt.SchoolKey is not null   
									and dschool.SchoolKey = dt.SchoolKey


WHERE  cdce.Date >= ssa.EntryDate 
   and cdce.Date <= GETDATE()
   and (
         (ssa.ExitWithdrawDate is null) 
	      OR
         (ssa.ExitWithdrawDate is not null and cdce.Date<=ssa.ExitWithdrawDate) 
	   )
	and ssa.SchoolYear = 2020 
	and dt.SchoolKey = 117
order by s.StudentUSI;

select * from BPS_DW.[dbo].[FactStudentAttendanceBySection]

--updatng the lineage table
UPDATE BPS_DW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;

/*
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
*/
--1
--select * from [EdFi_BPS_Staging_Ods].edfi.EducationOrganization

