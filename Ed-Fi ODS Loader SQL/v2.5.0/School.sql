SELECT 'Ed-Fi|' + Convert(nvarchar,s.SchoolId) [_sourceKey], ShortNameOfInstitution, NameOfInstitution
,sct.CodeValue SchoolCategoryType, sgld.CodeValue GradeLevelDescriptorCodeValue
,tIt.CodeValue TitleIPartASchoolDesignationTypeCodeValue
FROM edfi.School s
INNER JOIN edfi.EducationOrganization edorg on s.SchoolId = edorg.EducationOrganizationId
LEFT JOIN edfi.SchoolCategory sc on s.SchoolId = sc.SchoolId
LEFT JOIN edfi.SchoolCategoryType sct on sc.SchoolCategoryTypeId = sct.SchoolCategoryTypeId
LEFT JOIN edfi.TitleIPartASchoolDesignationType tIt on s.TitleIPartASchoolDesignationTypeId = tIt.TitleIPartASchoolDesignationTypeId
LEFT JOIN edfi.SchoolGradeLevel sgl on s.SchoolId = sgl.SchoolId
LEFT JOIN edfi.Descriptor sgld on sgl.GradeLevelDescriptorId = sgld.DescriptorId
