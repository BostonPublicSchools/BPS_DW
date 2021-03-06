--Deriving
--dropping the columnstore index
DROP INDEX IF EXISTS CSI_Derived_StudentAttendanceByDay ON Derived.StudentAttendanceByDay;

--ByDay
delete from [Derived].[StudentAttendanceByDay]
INSERT INTO [Derived].[StudentAttendanceByDay]
		   ([StudentKey]
		   ,[TimeKey]
		   ,[SchoolKey]
		   ,AttendanceEventCategoryKey
		   ,[EarlyDeparture]
		   ,[ExcusedAbsence]
		   ,[UnexcusedAbsence]
		   ,[NoContact]
		   ,[InAttendance]
		   ,[Tardy])

SELECT 
	   StudentKey, 
	   TimeKey, 
	   SchoolKey,
	   AttendanceEventCategoryKey,
	   --pivoted from row values	  
	   CASE WHEN [Early departure] IS NULL THEN 0 ELSE 1 END AS EarlyDeparture,
	   CASE WHEN [Excused Absence] IS NULL THEN 0 ELSE 1 END AS [ExcusedAbsence],
	   CASE WHEN [Unexcused Absence] IS NULL THEN 0 ELSE 1 END AS [UnexcusedAbsence],
	   CASE WHEN [No Contact] IS NULL THEN 0 ELSE 1 END AS [NoContact],
	   CASE WHEN [In Attendance] IS NULL THEN 0 ELSE 1 END AS [InAttendance],
	   CASE WHEN [Tardy] IS NULL THEN 0 ELSE 1 END AS [Tardy]	     

FROM (
		SELECT fsabd.StudentKey,
			   fsabd.TimeKey,
			   fsabd.SchoolKey,
			   fsabd.AttendanceEventCategoryKey,
			   dact.AttendanceEventCategoryDescriptor_CodeValue AS AttendanceType	       	 			 			   
		FROM dbo.[FactStudentAttendanceByDay] fsabd 
			 INNER JOIN dbo.DimStudent ds ON fsabd.StudentKey = ds.StudentKey
			 INNER JOIN dbo.DimAttendanceEventCategory dact ON fsabd.AttendanceEventCategoryKey = dact.AttendanceEventCategoryKey		
		WHERE 1=1 
		--AND ds.StudentUniqueId = 341888
		--AND dt.SchoolDate = '2018-10-26'


	) AS SourceTable 
PIVOT 
   (
	  MAX(AttendanceType)
	  FOR AttendanceType IN ([Early departure],
							 [Excused Absence],
							 [Unexcused Absence],
							 [No Contact],
							 [In Attendance],
							 [Tardy]
						)
   ) AS PivotTable;


--ADA
delete from  [Derived].[StudentAttendanceADA]
INSERT INTO [Derived].[StudentAttendanceADA]([StudentId]
													   ,[StudentStateId]
													   ,[FirstName]
													   ,[LastName]
													   ,[DistrictSchoolCode]
													   ,[UmbrellaSchoolCode]
													   ,[SchoolName]
													   ,[SchoolYear]
													   ,[NumberOfDaysPresent]
													   ,[NumberOfDaysAbsent]
													   ,[NumberOfDaysAbsentUnexcused]
													   ,[NumberOfDaysMembership]
													   ,[ADA])

SELECT    
		   v_sabd.StudentId, 
		   v_sabd.StudentStateId, 
		   v_sabd.FirstName, 
		   v_sabd.LastName, 
		   v_sabd.[DistrictSchoolCode],
		   v_sabd.[UmbrellaSchoolCode],	   
		   v_sabd.SchoolName, 	   
		   v_sabd.SchoolYear,	   
		   COUNT(DISTINCT (CASE WHEN v_sabd.InAttendance =1 THEN v_sabd.AttedanceDate ELSE NULL END))   AS NumberOfDaysPresent,
		   COUNT(DISTINCT (CASE WHEN v_sabd.InAttendance =0 THEN v_sabd.AttedanceDate ELSE NULL END))  AS NumberOfDaysAbsent,
		   COUNT(DISTINCT (CASE WHEN v_sabd.[UnexcusedAbsence] =1 THEN v_sabd.AttedanceDate ELSE NULL END))    AS NumberOfDaysAbsentUnexcused,
		   COUNT(DISTINCT v_sabd.AttedanceDate)   AS NumberOfDaysMembership,
		   COUNT(DISTINCT (CASE WHEN v_sabd.InAttendance =1 THEN v_sabd.AttedanceDate ELSE NULL END)) / CONVERT(Float,COUNT(DISTINCT v_sabd.AttedanceDate)) * 100 AS ADA
	--select DISTINCT v_sabd.AttedanceDate
	FROM dbo.View_StudentAttendanceByDay v_sabd
	--WHERE v_sabd.StudentId = 200369
		--AND v_sabd.SchoolYear = 2019
		--AND v_sabd.DistrictSchoolCode = 1120 
		--AND v_sabd.[UnexcusedAbsence] =0 
		--ORDER BY v_sabd.AttedanceDate 
	GROUP BY  v_sabd.StudentId, 
			  v_sabd.StudentStateId, 
			  v_sabd.FirstName, 
			  v_sabd.LastName, 
			  v_sabd.[DistrictSchoolCode],
			  v_sabd.[UmbrellaSchoolCode],	   
			  v_sabd.SchoolName, 	   
			  v_sabd.SchoolYear

CREATE COLUMNSTORE INDEX CSI_Derived_StudentAttendanceByDay
  ON Derived.StudentAttendanceByDay
  ([StudentKey]
	,[TimeKey]
	,[SchoolKey]
	,[EarlyDeparture]
	,[ExcusedAbsence]
	,[UnexcusedAbsence]
	,[NoContact]
	,[InAttendance]
	,[Tardy])
