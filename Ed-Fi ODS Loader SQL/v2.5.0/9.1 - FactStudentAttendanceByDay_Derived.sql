

TRUNCATE tablE [EdFiDW].[Derived].[StudentAttendanceByDay]
INSERT INTO [EdFiDW].[Derived].[StudentAttendanceByDay]
           ([StudentKey]
           ,[TimeKey]
           ,[SchoolKey]
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



