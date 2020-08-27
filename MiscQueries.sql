/*
I want to know the individual incidents for the school per year so that I can group 
by the school or by year by OSS and ISS, by race. 
*/

SELECT DISTINCT 
       [StudentId]
      ,[StudentStateId]
      ,[FirstName]
      ,[LastName]
      ,[DistrictSchoolCode]
      ,[UmbrellaSchoolCode]
      ,[SchoolName]
      ,[IncidentDate]
      ,[IncidentSchoolYear]     
      ,[IncidentIdentifier]
      ,[IncidentTime]
      ,[IncidentDescription]
      ,[IncidentType]
      ,[IncidentLocation]
      --,[IncidentAction] -- remov the action to get only unique incient records 
      ,[IncidentReporter]
      ,[IsISS]
      ,[IsOSS]
FROM EdFiDW.[dbo].[View_StudentDiscipline]
WHERE UmbrellaSchoolCode = '1040' -- Brighton High School
  AND IncidentSchoolYear = 2020

SELECT 
      DistrictSchoolCode,
	  SchoolName,
	  IncidentSchoolYear,
	  RaceCode,
      COUNT(DISTINCT [IncidentIdentifier]) AS TotalIncidents
FROM EdFiDW.dbo.View_StudentDiscipline
GROUP BY DistrictSchoolCode,
	     SchoolName,
	     IncidentSchoolYear,
		 RaceCode
ORDER BY SchoolName, IncidentSchoolYear, TotalIncidents


SELECT 
      DistrictSchoolCode,
	  SchoolName,
	  IncidentSchoolYear,
	  IsOSS,
      COUNT(DISTINCT [IncidentIdentifier]) AS TotalIncidents
FROM EdFiDW.dbo.View_StudentDiscipline
GROUP BY DistrictSchoolCode,
	     SchoolName,
	     IncidentSchoolYear,
		 IsOSS
ORDER BY SchoolName, IncidentSchoolYear, TotalIncidents



SELECT 
      DistrictSchoolCode,
	  SchoolName,
	  IncidentSchoolYear,
	  IsISS,
      COUNT(DISTINCT [IncidentIdentifier]) AS TotalIncidents
FROM EdFiDW.dbo.View_StudentDiscipline
GROUP BY DistrictSchoolCode,
	     SchoolName,
	     IncidentSchoolYear,
		 IsISS
ORDER BY SchoolName, IncidentSchoolYear, TotalIncidents



/*
I want to be able to see ADA for each student so that I can group 
by the school or by year by OSS and ISS, by race. 
*/

SELECT *
FROM EdFiDW.dbo.View_StudentAttendance_ADA
WHERE UmbrellaSchoolCode = '1040' -- Brighton High School
  AND SchoolYear = 2020
ORDER BY StudentId, SchoolYear


SELECT SchoolName,
       SchoolYear,	   
       AVG(ADA) AvgADA
FROM EdFiDW.dbo.View_StudentAttendance_ADA
GROUP BY SchoolName,
         SchoolYear
ORDER BY SchoolName, SchoolYear;