SELECT
	ssa.SchoolId,
	stu.StudentUSI,
	stu.StudentUniqueId,
	ssa.SchoolYear,
	ses.SessionName
	, count(*) DaysPresentAndEnrolledAsOfToday
	, ( SELECT count(*) 
		FROM edfi.StudentSchoolAttendanceEvent ssae 
		INNER JOIN edfi.Descriptor d on ssae.AttendanceEventCategoryDescriptorId = d.DescriptorId
		WHERE ssae.StudentUSI=ssa.StudentUSI  and ssae.SchoolId = ssa.SchoolId  
			and d.CodeValue not in ('In Attendance')
			and ses.BeginDate<= ssae.EventDate and ses.EndDate>=ssae.EventDate
	) DaysAbsent
FROM edfi.Student stu 
INNER JOIN edfi.StudentSchoolAssociation ssa on stu.StudentUSI = ssa.StudentUSI
INNER JOIN edfi.Descriptor egld on ssa.EntryGradeLevelDescriptorId = egld.DescriptorId
INNER JOIN edfi.Session ses on ssa.SchoolId = ses.SchoolId --and ssa.SchoolYear=ses.SchoolYear --and ses.SessionName like '%Semester%'
INNER JOIN edfi.SchoolYearType syt on ses.SchoolYear = syt.SchoolYear --and syt.SchoolYear=2020 --syt.CurrentSchoolYear=1 -- Current School Year
INNER JOIN edfi.CalendarDate cda on ses.SchoolId=cda.SchoolId and ses.BeginDate<=cda.Date and ses.EndDate>=cda.Date
INNER JOIN edfi.CalendarDateCalendarEvent cdce on cda.Date=cdce.Date and cda.SchoolId=cdce.SchoolId
INNER JOIN edfi.Descriptor cdet on cdce.CalendarEventDescriptorId = cdet.DescriptorId and cdet.CodeValue='Instructional day' -- ONLY Instructional days
where cdce.Date >= ssa.EntryDate -- Start from the student's enrollment entry date
	  and cdce.Date <= GETDATE()
and ( (ssa.ExitWithdrawDate is null) or (ssa.ExitWithdrawDate is not null and cdce.Date<=ssa.ExitWithdrawDate) )
group by stu.StudentUSI,stu.StudentUniqueId, ssa.SchoolId,egld.CodeValue,  ssa.SchoolYEar, syt.SchoolYear, ses.SchoolYear, ses.SessionName, ses.BeginDate, ses.EndDate--, --, ssa.EntryDate, ssa.ExitWithdrawDate
order by  StudentUSI;