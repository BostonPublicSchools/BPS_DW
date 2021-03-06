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

--truncate table EdFiDW.[dbo].[FactStudentAttendanceByDay]

INSERT INTO EdFiDW.[dbo].[FactStudentAttendanceByDay]
           ([StudentKey]
           ,[TimeKey]
           ,[SchoolKey]
           ,[AttendanceEventCategoryKey]
           ,[AttendanceEventReason]
           ,[LineageKey])
SELECT  
      ds.StudentKey,
      dt.TimeKey,	  
	  dschool.SchoolKey,      
	  daec.AttendanceEventCategoryKey,
	  'N/A' AS  AttendanceEventReason,
	  @lineageKey AS [LineageKey]
--select top 100  a.*
FROM [BPSGranary02].[BPSDW].[dbo].[Attendance] a	
	--joining DW tables
	INNER JOIN EdFiDW.dbo.DimStudent ds  ON CONCAT_WS('|', 'LegacyDW', Convert(NVARCHAR(MAX),a.[StudentNo]))   = ds._sourceKey
	                                   AND a.[Date] BETWEEN ds.ValidFrom AND ds.ValidTo
	INNER JOIN EdFiDW.dbo.DimSchool dschool ON CONCAT_WS('|', 'Ed-Fi', Convert(NVARCHAR(MAX),a.Sch))   = dschool._sourceKey -- all schools except one (inactive) are Ed-Fi
	                                   AND a.[Date] BETWEEN dschool.ValidFrom AND dschool.ValidTo
	INNER JOIN EdFiDW.dbo.DimTime dt ON a.[Date] = dt.SchoolDate
	                                and dt.SchoolKey is not null   
									and dschool.SchoolKey = dt.SchoolKey
    INNER JOIN EdFiDW.[dbo].DimAttendanceEventCategory daec ON CASE 
	                                                                WHEN a.AttendanceCodeDesc IN ('Absent') THEN 'Unexcused Absence'
	                                                                WHEN a.AttendanceCodeDesc IN ('Absent, Bus Strike','Bus / Transportation','Excused Absent','In School, Suspended','Suspended') THEN 'Excused Absence'
																	WHEN a.AttendanceCodeDesc IN ('Early Dismissal','Dismissed')  THEN 'Early departure'
																	WHEN a.AttendanceCodeDesc = 'No Contact'  THEN 'No Contact'
																	WHEN CHARINDEX('Tardy',a.AttendanceCodeDesc,1) > 0 THEN 'Tardy'
																	ELSE 'In Attendance' 	                                                                   
                                                                END = daec.AttendanceEventCategoryDescriptor_CodeValue
 
WHERE  a.[Date] >= '2015-07-01'



--select * from EdFiDW.[dbo].[FactStudentAttendanceByDay]

--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;
