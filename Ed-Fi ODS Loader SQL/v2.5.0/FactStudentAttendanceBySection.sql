SELECT
	ssa.SchoolId,
	stu.StudentUSI,
	stu.StudentUniqueId,
	ssa.SchoolYear,
	ses.SessionName
	, count(*) DaysPresentAndEnrolledAsOfToday
	, ( SELECT count(*) 
		FROM v25_EdFi_Ods_Populated_Template.edfi.StudentSchoolAttendanceEvent ssae 
		INNER JOIN v25_EdFi_Ods_Populated_Template.edfi.Descriptor d on ssae.AttendanceEventCategoryDescriptorId = d.DescriptorId
		WHERE ssae.StudentUSI=ssa.StudentUSI  and ssae.SchoolId = ssa.SchoolId  
			and d.CodeValue not in ('In Attendance')
			and ses.BeginDate<= ssae.EventDate and ses.EndDate>=ssae.EventDate
	) DaysAbsent
FROM v25_EdFi_Ods_Populated_Template.edfi.Student stu 
INNER JOIN v25_EdFi_Ods_Populated_Template.edfi.StudentSchoolAssociation ssa on stu.StudentUSI = ssa.StudentUSI
INNER JOIN v25_EdFi_Ods_Populated_Template.edfi.Descriptor egld on ssa.EntryGradeLevelDescriptorId = egld.DescriptorId
INNER JOIN v25_EdFi_Ods_Populated_Template.edfi.Session ses on ssa.SchoolId = ses.SchoolId --and ssa.SchoolYear=ses.SchoolYear --and ses.SessionName like '%Semester%'
INNER JOIN v25_EdFi_Ods_Populated_Template.edfi.SchoolYearType syt on ses.SchoolYear = syt.SchoolYear --and syt.SchoolYear=2020 --syt.CurrentSchoolYear=1 -- Current School Year
INNER JOIN v25_EdFi_Ods_Populated_Template.edfi.CalendarDate cda on ses.SchoolId=cda.SchoolId and ses.BeginDate<=cda.Date and ses.EndDate>=cda.Date
INNER JOIN v25_EdFi_Ods_Populated_Template.edfi.CalendarDateCalendarEvent cdce on cda.Date=cdce.Date and cda.SchoolId=cdce.SchoolId
INNER JOIN v25_EdFi_Ods_Populated_Template.edfi.Descriptor cdet on cdce.CalendarEventDescriptorId = cdet.DescriptorId and cdet.CodeValue='Instructional day' -- ONLY Instructional days
where cdce.Date >= ssa.EntryDate -- Start from the student's enrollment entry date
	  and cdce.Date <= GETDATE()
and ( (ssa.ExitWithdrawDate is null) OR
      (ssa.ExitWithdrawDate is not null and cdce.Date<=ssa.ExitWithdrawDate) )
group by stu.StudentUSI,stu.StudentUniqueId, ssa.SchoolId,egld.CodeValue,  ssa.SchoolYEar, syt.SchoolYear, ses.SchoolYear, ses.SessionName, ses.BeginDate, ses.EndDate--, --, ssa.EntryDate, ssa.ExitWithdrawDate
order by  StudentUSI;

SELECT * FROM v25_EdFi_Ods_Populated_Template.edfi.StudentSectionAssociation

SELECT TOP 5 * FROM v25_EdFi_Ods_Populated_Template.edfi.Staff

SELECT TOP 5 * FROM v25_EdFi_Ods_Populated_Template.edfi.Section

SELECT TOP 5 * FROM v25_EdFi_Ods_Populated_Template.edfi.StudentSectionAssociation


SELECT TOP 5 * FROM v32_EdFi_Ods_Populated_Template.edfi.Section

SELECT TOP 5 * FROM v32_EdFi_Ods_Populated_Template.edfi.SectionClassPeriod


SELECT TOP 5 * FROM v32_EdFi_Ods_Populated_Template.edfi.Staff