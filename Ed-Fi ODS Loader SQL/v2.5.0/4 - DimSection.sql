DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM BPS_DW.[dbo].[Lineage] WHERE TableName= 'dbo.DimSection')
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
	('dbo.DimSection', 
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
	 WHERE TableName= 'dbo.DimSection'
END 

INSERT INTO BPS_DW.[dbo].[DimSection]
           ([_sourceKey]
           ,[SchoolYear]
           ,[SchoolKey]
           ,[ShortNameOfInstitution]
           ,[NameOfInstitution]
           ,[ClassPeriodName]
           ,[ClassroomIdentificationCode]
           ,[LocalCourseCode]
           ,[SchoolTermDescriptor_CodeValue]
           ,[SchoolTermDescriptor_Description]
		 
           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])
    


select  distinct 
         'Ed-Fi|' + Convert(NVARCHAR(MAX),s.UniqueSectionCode) AS [_sourceKey]
        ,s.SchoolYear
		,ds.SchoolKey
		,ds.ShortNameOfInstitution
		,ds.NameOfInstitution
		,s.ClassPeriodName
		,s.ClassroomIdentificationCode
		,s.LocalCourseCode
		,d_t.CodeValue as TermDescriptorCodeValue
		,d_t.Description as TermDescriptorDescription
        ,GETDATE() AS ValidFrom
	    ,case when s.SchoolYear = BPS_DW.dbo.Func_GetSchoolYear(GETDATE()) then  '12/31/9999' else GETDATE() END AS ValidTo
	    ,case when s.SchoolYear = BPS_DW.dbo.Func_GetSchoolYear(GETDATE()) then  1 else 0 end AS IsCurrent
	    ,@lineageKey AS [LineageKey]
FROM EdFi_BPS_Staging_Ods.edfi.Section s 
     INNER JOIN BPS_DW.dbo.DimSchool ds ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),s.SchoolId)   = ds._sourceKey
	 INNER JOIN [EdFi_BPS_Staging_Ods].edfi.Descriptor d_t ON s.TermDescriptorId = d_t.DescriptorId
WHERE NOT EXISTS(SELECT 1 
					FROM BPS_DW.[dbo].[DimSection] ds 
					WHERE 'Ed-Fi|' + Convert(NVARCHAR(MAX),s.UniqueSectionCode) = ds._sourceKey)
	  AND s.SchoolYear IN (2019,2020);

--SELECT * FROM BPS_DW.[dbo].[DimSection]


--updatng the lineage table
UPDATE BPS_DW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;










	






