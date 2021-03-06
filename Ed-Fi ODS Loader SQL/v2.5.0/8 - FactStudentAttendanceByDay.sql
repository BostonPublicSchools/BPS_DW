DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM EdFiDW.[dbo].[ETL_Lineage] WHERE TableName= 'dbo.FactStudentAttendanceByDay')
BEGIN
    INSERT INTO EdFiDW.[dbo].[ETL_Lineage]
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
     SELECT LineageKey
	 FROM EdFiDW.[dbo].[ETL_Lineage]	 
	 WHERE TableName= 'dbo.FactStudentAttendanceByDay'
END 

truncate table EdFiDW.[dbo].[FactStudentAttendanceByDay]
;WITH DistinctAttedanceEvents AS
(
	SELECT   DISTINCT 
			 StudentUSI, 
			 SchoolId, 
			 SchoolYear, 
			 EventDate,
			 AttendanceEventCategoryDescriptorId,
			 LTRIM(RTRIM(COALESCE(AttendanceEventReason,''))) AS AttendanceEventReason 
	FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAttendanceEvent
	WHERE SchoolYear IN (2019,2020,2021)
 ),
AttedanceEvents AS
(
	SELECT  StudentUSI, 
			SchoolId, 
			SchoolYear, 
			EventDate,
			AttendanceEventCategoryDescriptorId,
			AttendanceEventReason , 
			ROW_NUMBER() OVER (PARTITION BY StudentUSI, 
											SchoolId, 
											SchoolYear, 
											EventDate,
											AttendanceEventCategoryDescriptorId
								ORDER BY AttendanceEventReason DESC) AS RowId 
	FROM DistinctAttedanceEvents
)
			
INSERT INTO EdFiDW.[dbo].[FactStudentAttendanceByDay]
           ([_sourceKey]
		   ,[StudentKey]
           ,[TimeKey]
           ,[SchoolKey]
           ,[AttendanceEventCategoryKey]
           ,[AttendanceEventReason]
           ,[LineageKey])

SELECT DISTINCT 
      '',
      ds.StudentKey,
      dt.TimeKey,	  
	  dschool.SchoolKey,      
	  COALESCE(daec.AttendanceEventCategoryKey,(SELECT TOP 1 AttendanceEventCategoryKey FROM EdFiDW.[dbo].DimAttendanceEventCategory WHERE AttendanceEventCategoryDescriptor_CodeValue = 'In Attendance')) AS AttendanceEventCategoryKey,	       
	  COALESCE(ssae.AttendanceEventReason,'N/A') AS  AttendanceEventReason,
	  @lineageKey AS [LineageKey]
--select *  
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
    INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CalendarDate cda on ssa.SchoolId = cda.SchoolId 														   
	INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CalendarDateCalendarEvent cdce on cda.Date=cdce.Date 
																		 and cda.SchoolId=cdce.SchoolId
	INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d_cdce on cdce.CalendarEventDescriptorId = d_cdce.DescriptorId
														  and d_cdce.CodeValue='Instructional day' -- ONLY Instructional days
	
	LEFT JOIN AttedanceEvents ssae on ssa.StudentUSI = ssae.StudentUSI
	                                               AND ssa.SchoolId = ssae.SchoolId 
												   AND cda.Date = ssae.EventDate
												   AND ssae.RowId= 1			
	--joining DW tables
	INNER JOIN EdFiDW.dbo.DimStudent ds  ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ssa.StudentUSI)   = ds._sourceKey
	                                                                     AND cdce.Date BETWEEN ds.ValidFrom AND ds.ValidTo
	INNER JOIN EdFiDW.dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ssa.SchoolId)   = dschool._sourceKey
	                                        AND cdce.Date BETWEEN dschool.ValidFrom AND dschool.ValidTo
	INNER JOIN EdFiDW.dbo.DimTime dt ON cdce.Date = dt.SchoolDate
	                                and dt.SchoolKey is not null   
									and dschool.SchoolKey = dt.SchoolKey
    LEFT JOIN EdFiDW.[dbo].DimAttendanceEventCategory daec ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ssae.AttendanceEventCategoryDescriptorId)  = daec._sourceKey
	                                                       
 
WHERE  cdce.Date >= ssa.EntryDate 
   and cdce.Date <= GETDATE()
   and (
         (ssa.ExitWithdrawDate is null) 
	      OR
         (ssa.ExitWithdrawDate is not null and cdce.Date<=ssa.ExitWithdrawDate) 
	   )
	and ssa.SchoolYear IN (2019,2020,2021);

--select top 1 * from EdFiDW.[dbo].[FactStudentAttendanceByDay]

--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;



