USE EdFiDW
GO
DECLARE @dropExistingTables BIT = 1

--creating schemas  if they do not exist
-------------------------------------------------------------
DECLARE @sqlCmd NVARCHAR(max);
IF NOT EXISTS (SELECT 1 
                FROM sys.schemas 
				WHERE [name] = 'Staging')
		BEGIN
			SET @sqlCmd = 'CREATE SCHEMA [Staging] AUTHORIZATION dbo';
			EXEC sp_executesql @sqlCmd;
        END   

IF NOT EXISTS (SELECT 1 
                FROM sys.schemas 
				WHERE [name] = 'Raw_EdFi')
		BEGIN
			SET @sqlCmd = 'CREATE SCHEMA [Raw_EdFi] AUTHORIZATION dbo';
			EXEC sp_executesql @sqlCmd;
        END 
  
IF NOT EXISTS (SELECT 1 
                FROM sys.schemas 
				WHERE [name] = 'Raw_LegacyDW')
		BEGIN
			SET @sqlCmd = 'CREATE SCHEMA [Raw_LegacyDW] AUTHORIZATION dbo';
			EXEC sp_executesql @sqlCmd;
        END 

IF NOT EXISTS (SELECT 1 
                FROM sys.schemas 
				WHERE [name] = 'Derived')
		BEGIN
			SET @sqlCmd = 'CREATE SCHEMA [Derived] AUTHORIZATION dbo';
			EXEC sp_executesql @sqlCmd;
        END 

--dropping all db objects
IF (@dropExistingTables = 1)
BEGIN

   --views - dropping views first as they are schema bound
   ---------------------------------------------------------------
   DROP VIEW IF EXISTS dbo.View_StudentAssessmentScores;
   DROP VIEW IF EXISTS dbo.View_StudentAttendance_ADA;
   DROP VIEW IF EXISTS dbo.View_StudentAttendanceByDay;
   DROP VIEW IF EXISTS dbo.View_StudentDiscipline;
   DROP VIEW IF EXISTS dbo.View_StudentCourseTranscript;
   DROP VIEW IF EXISTS dbo.View_StudentRoster;
    
   --fact tables
   ---------------------------------------------------------------
   DROP TABLE IF EXISTS dbo.FactStudentAttendanceByDay;
   DROP TABLE IF EXISTS dbo.FactStudentAssessmentScore;
   DROP TABLE IF EXISTS dbo.FactStudentDiscipline;
   DROP TABLE IF EXISTS dbo.FactStudentCourseTranscript;
    
 
	   
   --ETL Objects
   ---------------------------------------------------------------
   --tables
   DROP TABLE IF EXISTS dbo.ETL_Lineage;
   DROP TABLE IF EXISTS dbo.ETL_IncrementalLoads;
   DROP TABLE IF EXISTS Staging.School;
   DROP TABLE IF EXISTS Staging.[Time];
   DROP TABLE IF EXISTS Staging.[Staff];
   DROP TABLE IF EXISTS Staging.Student;
   DROP TABLE IF EXISTS Staging.AttendanceEventCategory;
   DROP TABLE IF EXISTS Staging.DisciplineIncident;
   DROP TABLE IF EXISTS Staging.Assessment;
   DROP TABLE IF EXISTS Staging.Course;

   DROP TABLE IF EXISTS Staging.StudentAttendanceByDay;
   DROP TABLE IF EXISTS Staging.StudentDiscipline;
   DROP TABLE IF EXISTS Staging.StudentAssessmentScore;
   DROP TABLE IF EXISTS Staging.StudentCourseTranscript;

   --functions
   DROP FUNCTION IF EXISTS dbo.Func_ETL_GetFullName;
   DROP FUNCTION IF EXISTS dbo.Func_ETL_GetHolidayFromDate;
   DROP FUNCTION IF EXISTS dbo.Func_ETL_GetSchoolYear;
   DROP FUNCTION IF EXISTS dbo.Func_ETL_GetEasterHolidays;

   --stored procedures
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_Lineage_GetKey];   
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_IncrementalLoads_GetLastLoadedDate];

   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_DimSchool_PopulateStaging];
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_DimSchool_PopulateProduction];
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_DimTime_PopulateStaging];
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_DimTime_PopulateProduction];
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_DimStudent_PopulateStaging];
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_DimStudent_PopulateProduction];
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_DimAttendanceEventCategory_PopulateStaging];
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_DimAttendanceEventCategory_PopulateProduction];
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_DimDisciplineIncident_PopulateStaging];
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_DimDisciplineIncident_PopulateProduction];
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_DimAssessment_PopulateStaging];
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_DimAssessment_PopulateProduction];
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_DimCourse_PopulateStaging];
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_DimCourse_PopulateProduction];

   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_FactStudentAttendanceByDay_PopulateProduction];
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_FactStudentDiscipline_PopulateProduction];
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_FactStudentAssessmentScore_PopulateProduction];
   DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_FactStudentCourseTranscript_PopulateProduction];
   

   --derived tables
   ---------------------------------------------------------------
   DROP TABLE IF EXISTS Derived.StudentAttendanceByDay; 
   DROP TABLE IF EXISTS Derived.StudentAttendanceADA; 
   DROP TABLE IF EXISTS Derived.StudentAssessmentScore;
   DROP TABLE IF EXISTS Derived.StaffCurrentSchools;
   DROP TABLE IF EXISTS Derived.StaffCurrentGradeLevels;
   DROP TABLE IF EXISTS Derived.StaffCurrentStudents;

     --dim tables
   ---------------------------------------------------------------
   DROP TABLE IF EXISTS dbo.DimCourse;
   DROP TABLE IF EXISTS dbo.DimAssessment;
   DROP TABLE IF EXISTS dbo.DimDisciplineIncident;
   DROP TABLE IF EXISTS dbo.DimAttendanceEventCategory;
   DROP TABLE IF EXISTS dbo.DimStudent;
   DROP TABLE IF EXISTS dbo.DimTime;
   DROP TABLE IF EXISTS dbo.DimStaff;
   DROP TABLE IF EXISTS dbo.DimSchool;
END;


--DIMENSION TABLES
-------------------------------------------------------------------------------------------------
--school
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimSchool' 
			   AND TABLE_SCHEMA = 'dbo')
BEGIN
	CREATE TABLE dbo.DimSchool
	(
	  SchoolKey int NOT NULL IDENTITY(1,1), -- surrogate
	  [_sourceKey] NVARCHAR(50) NOT NULL,  --'Ed-Fi|Id'
	  DistrictSchoolCode NVARCHAR(10) NULL ,
	  StateSchoolCode NVARCHAR(50) NULL ,
	  UmbrellaSchoolCode NVARCHAR(50) NULL,

	  ShortNameOfInstitution NVARCHAR(500) NOT NULL,     
	  NameOfInstitution NVARCHAR(500) NOT NULL,    

	  SchoolCategoryType NVARCHAR(100) NOT NULL,     -- elem, middle, hs, combined
	  SchoolCategoryType_Elementary_Indicator BIT NOT NULL,      
	  SchoolCategoryType_Middle_Indicator BIT NOT NULL,
	  SchoolCategoryType_HighSchool_Indicator BIT NOT NULL,    
	  SchoolCategoryType_Combined_Indicator BIT NOT NULL,    
	  SchoolCategoryType_Other_Indicator BIT NOT NULL,    
  
	  TitleIPartASchoolDesignationTypeCodeValue  NVARCHAR(50) NOT NULL,--Not designated as a Title I Part A school
																		--Title I Part A Schoolwide Assistance Program Schoo
																		--Title I Part A Targeted Assistance School
																		--Title I targeted eligible school - no program
																		--Title I targeted school
																		--Title I school wide eligible - Title I targeted pr
																		--Title I school wide eligible school - no program
                                            
	  TitleIPartASchoolDesignation_Indicator BIT NOT NULL, -- True,False
	  OperationalStatusTypeDescriptor_CodeValue NVARCHAR(50) NOT NULL,
	  OperationalStatusTypeDescriptor_Description NVARCHAR(1024) NOT NULL,
  

	  ValidFrom DATETIME NOT NULL,
	  ValidTo DATETIME NOT NULL,
	  IsCurrent BIT NOT NULL,  
	  [LineageKey] INT NOT NULL,

	  CONSTRAINT PK_DimSchool PRIMARY KEY (SchoolKey)  
	);

	CREATE NONCLUSTERED INDEX DimSchool_CoveringIndex
	  ON dbo.DimSchool (_sourceKey, ValidFrom)
	INCLUDE ( ValidTo, SchoolKey);
END;


--time
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimTime' 
			   AND TABLE_SCHEMA = 'dbo')
BEGIN
	CREATE TABLE dbo.DimTime
	(
	  TimeKey INT NOT NULL IDENTITY(1,1), -- ex 9/1/2019 : 20190901 -- surrogate    
  
	  SchoolDate DATE NOT NULL , -- 9/1/2019
	  SchoolDate_MMYYYY CHAR(6) NOT NULL,
	  SchoolDate_Fomat1 CHAR(10) NOT NULL,
	  SchoolDate_Fomat2 CHAR(8) NOT NULL,
	  SchoolDate_Fomat3 CHAR(10) NOT NULL,
    
	  SchoolYear SMALLINT NOT NULL, -- ex: 9/1/2019 = 2020  
	  SchoolYearDescription NVARCHAR(50) NOT NULL, -- '2019-2020 or SchoolYear 2019 - 2020'
	  CalendarYear SMALLINT NOT NULL, -- ex: 9/1/2019 = 2020
  
	  DayOfMonth TINYINT NOT NULL, -- 1 - 30|31
	  DaySuffix CHAR(2) NOT NULL , -- 1st, 2nd, 3rd
	  DayName  NVARCHAR(15) NOT NULL, -- Monday, Tuesday    
	  DayNameShort NVARCHAR(15) NOT NULL, -- Mon, Tue
	  DayOfWeek TINYINT NOT NULL, -- 1 - 7
  
	  WeekInMonth TINYINT NOT NULL, -- 1 - 4 or 5 -- this counts 7 days starting on the 1st of the month
	  WeekOfMonth TINYINT NOT NULL, -- 1 - 4 or 5 -- this is the actual week of the month (starting on sunday)
	  Weekend_Indicator BIT NOT NULL,
	  WeekOfYear TINYINT NOT NULL, -- 1 - 53   
	  FirstDayOfWeek DATE NOT NULL,
	  LastDayOfWeek DATE NOT NULL,
	  WeekBeforeChristmas_Indicator BIT NOT NULL, --  True,False
  
  
	  [Month] TINYINT NOT NULL, -- 1..12
	  MonthName  NVARCHAR(10) NOT NULL, --January,February,December
	  MonthNameShort CHAR(3) NOT NULL, --Jan,Feb,Dec  
	  FirstDayOfMonth DATE NOT NULL,
	  LastDayOfMonth DATE NOT NULL,
	  FirstDayOfNextMonth DATE NOT NULL,
	  LastDayOfNextMonth DATE NOT NULL,
    
	  DayOfYear SMALLINT NULL, -- 1 - 365 or 366 (Leap Year Every Four Years)  
  
	  LeapYear_Indicator BIT NOT NULL,  
    
	  FederalHolidayName NVARCHAR(20) NULL, -- Memorial Day
	  FederalHoliday_Indicator BIT NOT NULL, --  True,False
  
	  --all these vary by school
	  SchoolKey INT NULL,  
	  DayOfSchoolYear SMALLINT NULL, -- 1 - 180 - based on SIS(ODS) school calendar
	  SchoolCalendarEventType_CodeValue NVARCHAR(50) NULL, -- Emergency day,Instructional day,Teacher only day
	  SchoolCalendarEventType_Description NVARCHAR(50) NULL, -- Emergency day,Instructional day,Teacher only day
    
	  SchoolTermDescriptor_CodeValue NVARCHAR(50) NULL, -- Year Round,First Quarter, First Trimester, Fall Semester, Fourth Quarter, etc.  SELECT * FROM v25_EdFi_Ods_Sandbox_populatedSandbox.edfi.Descriptor where namespace = 'http://ed-fi.org/Descriptor/TermDescriptor.xml'
	  SchoolTermDescriptor_Description NVARCHAR(50) NULL, -- Year Round,First Quarter, First Trimester, Fall Semester, Fourth Quarter, etc SELECT * FROM v25_EdFi_Ods_Sandbox_populatedSandbox.edfi.Descriptor where namespace = 'http://ed-fi.org/Descriptor/TermDescriptor.xml'

	  --all indicators were removed until we see actual district's data.   
  
	  ValidFrom DATETIME NOT NULL,
	  ValidTo DATETIME NOT NULL,
	  IsCurrent BIT NOT NULL,  
	  [LineageKey] INT NOT NULL,

	  CONSTRAINT PK_DimTime PRIMARY KEY (TimeKey),  
	  CONSTRAINT FK_DimTime_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES [dbo].[DimSchool] (SchoolKey)
	);

	CREATE NONCLUSTERED INDEX DimTime_CoveringIndex
	  ON dbo.DimTime (SchoolDate, ValidFrom)
	INCLUDE ( ValidTo, TimeKey);
END;

if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimStaff' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.DimStaff
(
  StaffKey INT NOT NULL IDENTITY(1,1),
  [_sourceKey] NVARCHAR(50) NOT NULL,  --'EdFi|StaffUSI'
  
  PrimaryElectronicMailAddress [nvarchar](128) NULL,
  PrimaryElectronicMailTypeDescriptor_CodeValue [nvarchar](128) NULL, -- Home/Personal, Organization, Other, Work
  PrimaryElectronicMailTypeDescriptor_Description [nvarchar](128) NULL,
  
  [StaffUniqueId] [nvarchar](32) NOT NULL,
  [PersonalTitlePrefix] [nvarchar](30) NULL,
  [FirstName] [nvarchar](75) NOT NULL,
  [MiddleName] [nvarchar](75) NULL,
  [MiddleInitial] CHAR(1) NULL,
  [LastSurname] nvarchar(75) NOT NULL,
  [FullName] NVARCHAR(50) NOT NULL,
  [GenerationCodeSuffix] [nvarchar](10) NULL,
  [MaidenName] [nvarchar](75) NULL,  
  [BirthDate] DATE NULL,
  [StaffAge] INT NULL,  
  
  SexType_Code NVARCHAR(15) NOT NULL,
  SexType_Description NVARCHAR(100) NOT NULL,    
  SexType_Male_Indicator BIT NOT NULL,
  SexType_Female_Indicator BIT NOT NULL,
  SexType_NotSelected_Indicator BIT NOT NULL,
     
  [HighestLevelOfEducationDescriptorDescriptor_CodeValue] NVARCHAR(100)  NULL, 
  [HighestLevelOfEducationDescriptorDescriptor_Description] NVARCHAR(100)  NULL, 
  [YearsOfPriorProfessionalExperience] [decimal](5, 2) NULL,
  [YearsOfPriorTeachingExperience] [decimal](5, 2) NULL,  
  [HighlyQualifiedTeacher_Indicator] [bit] NULL,
    
  [StaffClassificationDescriptor_CodeValue]  NVARCHAR(100)  NULL, 
  [StaffClassificationDescriptor_CodeDescription]  NVARCHAR(100)  NULL, 

  ValidFrom DATETIME NOT NULL, 
  ValidTo DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL,    
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimStaff PRIMARY KEY (StaffKey)  
);

--student
if NOT EXISTS (select 1
               FROM INFORMATION_SCHEMA.TABLES
               WHERE TABLE_NAME = 'DimStudent' 
			     AND TABLE_SCHEMA = 'dbo')
BEGIN
	CREATE TABLE dbo.DimStudent
	(
		[StudentKey] [int] IDENTITY(1,1) NOT NULL, 
		[_sourceKey] [nvarchar](50) NOT NULL,

		[StudentUniqueId] [nvarchar](32) NULL,
		[StateId] [nvarchar](32) NULL,

		PrimaryElectronicMailAddress [nvarchar](128) NULL,
		PrimaryElectronicMailTypeDescriptor_CodeValue [nvarchar](128) NULL, -- Home/Personal, Organization, Other, Work
		PrimaryElectronicMailTypeDescriptor_Description [nvarchar](128) NULL,

		[SchoolKey] [int] NOT NULL,
		[ShortNameOfInstitution] [nvarchar](500) NOT NULL,
		[NameOfInstitution] [nvarchar](500) NOT NULL,
		[GradeLevelDescriptor_CodeValue] [nvarchar](100) NOT NULL,
		[GradeLevelDescriptor_Description] [nvarchar](500) NOT NULL,	
	
		[FirstName] [nvarchar](100) NOT NULL,
		[MiddleInitial] [char](1) NULL,
		[MiddleName] [nvarchar](100) NULL,
		[LastSurname] [nvarchar](100) NOT NULL,
		[FullName] [nvarchar](500) NOT NULL,
		[BirthDate] [date] NOT NULL,
		[StudentAge] [int] NOT NULL,
		[GraduationSchoolYear] [int] NULL,
	
		[Homeroom] [nvarchar](500) NULL,
		[HomeroomTeacher] [nvarchar](500) NULL,

		[SexType_Code] [nvarchar](100) NOT NULL,
		[SexType_Description] [nvarchar](100) NOT NULL,
		[SexType_Male_Indicator] [bit] NOT NULL,
		[SexType_Female_Indicator] [bit] NOT NULL,
		[SexType_NotSelected_Indicator] [bit] NOT NULL,
	
		[RaceCode] [nvarchar](1000) NOT NULL,
		[RaceDescription] [nvarchar](1000) NOT NULL,
		[StateRaceCode] [nvarchar](1000) NOT NULL,
		[Race_AmericanIndianAlaskanNative_Indicator] [bit] NOT NULL,
		[Race_Asian_Indicator] [bit] NOT NULL,
		[Race_BlackAfricaAmerican_Indicator] [bit] NOT NULL,
		[Race_NativeHawaiianPacificIslander_Indicator] [bit] NOT NULL,
		[Race_White_Indicator] [bit] NOT NULL,
		[Race_MultiRace_Indicator] [bit] NOT NULL,
		[Race_ChooseNotRespond_Indicator] [bit] NOT NULL,
		[Race_Other_Indicator] [bit] NOT NULL,

		[EthnicityCode] [nvarchar](100) NOT NULL,
		[EthnicityDescription] [nvarchar](100) NOT NULL,
		[EthnicityHispanicLatino_Indicator] [bit] NOT NULL,
		[Migrant_Indicator] [bit] NOT NULL,
		[Homeless_Indicator] [bit] NOT NULL,
		[IEP_Indicator] [bit] NOT NULL,
		[English_Learner_Code_Value] [nvarchar](100) NOT NULL,
		[English_Learner_Description] [nvarchar](100) NOT NULL,
		[English_Learner_Indicator] [bit] NOT NULL,
		[Former_English_Learner_Indicator] [bit] NOT NULL,
		[Never_English_Learner_Indicator] [bit] NOT NULL,
		[EconomicDisadvantage_Indicator] [bit] NOT NULL,
	
		[EntryDate] [datetime2](7) NOT NULL,
		[EntrySchoolYear] [int] NOT NULL,
		[EntryCode] [nvarchar](25) NOT NULL,
	
		[ExitWithdrawDate] [datetime2](7) NULL,
		[ExitWithdrawSchoolYear] [int] NULL,
		[ExitWithdrawCode] [nvarchar](100) NULL,

		[ValidFrom] [datetime] NOT NULL,
		[ValidTo] [datetime] NOT NULL,
		[IsCurrent] [bit] NOT NULL,
		[LineageKey] [int] NOT NULL,

		CONSTRAINT PK_DimStudent PRIMARY KEY (StudentKey)
		
	);

	CREATE NONCLUSTERED INDEX DimStudent_CoveringIndex
	  ON dbo.DimStudent (_sourceKey, ValidFrom)
	INCLUDE ( ValidTo, StudentKey);

    CREATE NONCLUSTERED INDEX IX_DimStudent_IsCurrent
    ON [dbo].[DimStudent] ([IsCurrent])



END;


--attendance event category
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimAttendanceEventCategory' 
			   AND TABLE_SCHEMA = 'dbo')
BEGIN
	CREATE TABLE dbo.DimAttendanceEventCategory
	(
	  AttendanceEventCategoryKey INT NOT NULL IDENTITY(1,1),
	  [_sourceKey] NVARCHAR(50) NOT NULL, 
   
	  AttendanceEventCategoryDescriptor_CodeValue nvarchar(50) NOT NULL,
	  AttendanceEventCategoryDescriptor_Description nvarchar(1024) NOT NULL,
  
	  [InAttendance_Indicator] BIT NOT NULL,  
	  [UnexcusedAbsence_Indicator] BIT NOT NULL,
	  [ExcusedAbsence_Indicator] BIT NOT NULL,  
	  [Tardy_Indicator] BIT NOT NULL,    
	  [EarlyDeparture_Indicator]  BIT NOT NULL,    

	  ValidFrom DATETIME NOT NULL, 
	  ValidTo DATETIME NOT NULL, 
	  IsCurrent BIT NOT NULL,    
	  [LineageKey] INT NOT NULL,

	  CONSTRAINT PK_DimAttendanceEventCategory PRIMARY KEY (AttendanceEventCategoryKey ASC)  
	);

	CREATE NONCLUSTERED INDEX DimAttendanceEventCategory_CoveringIndex
	  ON dbo.DimAttendanceEventCategory(_sourceKey, ValidFrom)
	INCLUDE ( ValidTo, AttendanceEventCategoryKey);
END;


--discipline incident
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimDisciplineIncident' 
			   AND TABLE_SCHEMA = 'dbo')
BEGIN
	CREATE TABLE dbo.DimDisciplineIncident
	(
	  DisciplineIncidentKey INT NOT NULL IDENTITY(1,1),
	  [_sourceKey] NVARCHAR(50) NOT NULL, 
  
	  SchoolKey INT NOT NULL,
	  ShortNameOfInstitution nvarchar(500) NOT NULL,
	  NameOfInstitution nvarchar(500) NOT NULL,
	  SchoolYear INT NOT NULL,
	  IncidentDate DATE NOT NULL,   
	  IncidentTime TIME(7) NOT NULL,   
	  [IncidentDescription] nvarchar(MAX) NULL ,
	  [BehaviorDescriptor_CodeValue] nvarchar(50) not null, -- IncidentType: Weapons Possession (Firearms and Other Weapons), Drugs, Abuse Of Volatile Chemical,School Code of Conduct,  etc
	  [BehaviorDescriptor_Description] nvarchar(1024) not null,

	  [LocationDescriptor_CodeValue] nvarchar(50) not null,  -- Hallway, Cafeteria, Classroom, etc
	  [LocationDescriptor_Description] nvarchar(1024) not null,

	  [DisciplineDescriptor_CodeValue] nvarchar(50) not null, -- Actions: Community Service, Expulsion,In School Suspension,Out of School Suspension, Removal from Classroom, etc
	  [DisciplineDescriptor_Description] nvarchar(1024) not null,
	  DisciplineDescriptor_ISS_Indicator BIT NOT NULL,
	  DisciplineDescriptor_OSS_Indicator BIT NOT NULL,

	  ReporterDescriptor_CodeValue nvarchar(50) NOT NULL, -- Law enforcement officer,Non-school personnel,Other,Parent/guardian,Staff,Student  
	  ReporterDescriptor_Description nvarchar(1024) NOT NULL,
  
	  IncidentReporterName NVARCHAR(100) NOT NULL ,
	  ReportedToLawEnforcement_Indicator BIT NOT NULL ,
	  IncidentCost Money NOT NULL,
  
	  ValidFrom DATETIME NOT NULL,
	  ValidTo DATETIME NOT NULL,
	  IsCurrent BIT NOT NULL,  
  
	  [LineageKey] INT NOT NULL,

	  CONSTRAINT PK_DimDisciplineIncident PRIMARY KEY (DisciplineIncidentKey ASC)
	  
	);

	CREATE NONCLUSTERED INDEX DimDisciplineIncident_CoveringIndex
	  ON dbo.DimDisciplineIncident(_sourceKey, ValidFrom)
	INCLUDE ( ValidTo, DisciplineIncidentKey);
END;

--assessment
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimAssessment' 
			   AND TABLE_SCHEMA = 'dbo')
BEGIN
	CREATE TABLE dbo.DimAssessment
	(
		AssessmentKey INT NOT NULL IDENTITY(1,1),  
		[_sourceKey] NVARCHAR(2000) NOT NULL, -- EdFi|AssessmentIdentifier|ObjectiveAssessment_IdentificationCode|AssessmentReportingMethodDescriptor_CodeValue
	
		--assessment 
		--------------------------------------------------------------------------------------
		AssessmentCategoryDescriptor_CodeValue NVARCHAR(50) NOT NULL,    
		AssessmentCategoryDescriptor_Description NVARCHAR(1024) NOT NULL,    
		AssessmentFamilyTitle NVARCHAR(100) NULL,    	
		AdaptiveAssessment_Indicator bit NOT NULL, 
		AssessmentIdentifier NVARCHAR(60) NOT NULL,   
		AssessmentTitle NVARCHAR(500) NOT NULL,


		ReportingMethodDescriptor_CodeValue NVARCHAR(50) NOT NULL,   
		ReportingMethodDescriptor_Description NVARCHAR(1024) NOT NULL,   
	
		ResultDatatypeTypeDescriptor_CodeValue  NVARCHAR(50) NOT NULL,   
		ResultDatatypeTypeDescriptor_Description NVARCHAR(1024) NOT NULL,   
	
		AssessmentScore_Indicator  BIT NOT NULL,
		AssessmentPerformanceLevel_Indicator  BIT NOT NULL,

		ObjectiveAssessmentScore_Indicator  BIT NOT NULL,
		ObjectiveAssessmentPerformanceLevel_Indicator  BIT NOT NULL,
	
		ValidFrom DATETIME NOT NULL, 
		ValidTo DATETIME NOT NULL, 
		IsCurrent BIT NOT NULL,	
		[LineageKey] INT NOT NULL,

		CONSTRAINT PK_DimAssessment PRIMARY KEY (AssessmentKey)    
	);

	CREATE NONCLUSTERED INDEX DimAssessment_CoveringIndex
	  ON dbo.DimAssessment(_sourceKey, ValidFrom)
	INCLUDE ( ValidTo, AssessmentKey);
END;

--course
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimCourse' 
			   AND TABLE_SCHEMA = 'dbo')
BEGIN
	CREATE TABLE dbo.DimCourse
	(
		CourseKey INT NOT NULL IDENTITY(1,1),  --surrogate
		[_sourceKey] NVARCHAR(50) NOT NULL,
	
		CourseCode NVARCHAR(60) NOT NULL,
		CourseTitle NVARCHAR(100) NOT NULL,
		CourseDescription NVARCHAR(100) NOT NULL,

		CourseLevelCharacteristicTypeDescriptor_CodeValue NVARCHAR(60) NOT NULL,
		CourseLevelCharacteristicTypeDescriptor_Description NVARCHAR(1024) NOT NULL, 

		AcademicSubjectDescriptor_CodeValue  NVARCHAR(60) NOT NULL,
		AcademicSubjectDescriptor_Description  NVARCHAR(1024) NOT NULL,

		HighSchoolCourseRequirement_Indicator BIT NOT NULL,
		MinimumAvailableCredits INT NULL,
		MaximumAvailableCredits INT NULL,
	
		GPAApplicabilityType_CodeValue NVARCHAR(50) NULL,
		GPAApplicabilityType_Description NVARCHAR(50) NULL,

		SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue NVARCHAR(50) NOT NULL,
		SecondaryCourseLevelCharacteristicTypeDescriptor_Description NVARCHAR(50) NOT NULL,
			
		ValidFrom DATETIME NOT NULL, 
		ValidTo DATETIME NOT NULL, 
		IsCurrent BIT NOT NULL,	
		[LineageKey] INT NOT NULL,

  		CONSTRAINT PK_DimCourse PRIMARY KEY (CourseKey)
    
	);


	CREATE NONCLUSTERED INDEX DimCourse_CoveringIndex
	  ON dbo.DimCourse(_sourceKey, ValidFrom)
	INCLUDE ( ValidTo, CourseKey);
END


--FACT TABLES
----------------------------------------------------------------------
--attendance by day
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentAttendanceByDay' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.FactStudentAttendanceByDay
(
  [_sourceKey] NVARCHAR(50) NOT NULL,
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL,  
  SchoolKey INT NOT NULL,
  AttendanceEventCategoryKey INT NOT NULL ,
  AttendanceEventReason nvarchar(500) NOT NULL,

  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentAttendanceByDay PRIMARY KEY (StudentKey ASC, TimeKey ASC, SchoolKey ASC, AttendanceEventCategoryKey ASC),
  CONSTRAINT FK_FactStudentAttendanceByDay_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentAttendanceByDay_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),  
  CONSTRAINT FK_FactStudentAttendanceByDay_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey),
  CONSTRAINT FK_FactStudentAttendanceByDay_AttendanceEventCategoryKey FOREIGN KEY (AttendanceEventCategoryKey) REFERENCES dbo.DimAttendanceEventCategory(AttendanceEventCategoryKey)   
);

--discipline -- v2
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentDiscipline' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.FactStudentDiscipline
(
  [_sourceKey] NVARCHAR(50) NOT NULL,
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL, 
  SchoolKey INT NOT NULL,
  DisciplineIncidentKey INT NOT NULL,

  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentDiscipline PRIMARY KEY (StudentKey ASC, TimeKey ASC, SchoolKey ASC, DisciplineIncidentKey ASC),
  CONSTRAINT FK_FactStudentDiscipline_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentDiscipline_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentDiscipline_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey),
  CONSTRAINT FK_FactStudentDiscipline_DisciplineIncidentKey FOREIGN KEY (DisciplineIncidentKey) REFERENCES dbo.DimDisciplineIncident(DisciplineIncidentKey)
);


if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentAssessmentScore' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.FactStudentAssessmentScore
(
  [_sourceKey] NVARCHAR(50) NOT NULL,
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL, 
  AssessmentKey INT NOT NULL,
  ScoreResult   NVARCHAR(50) NOT NULL,
  IntegerScoreResult INT NULL,
  DecimalScoreResult FLOAT NULL,
  LiteralScoreResult NVARCHAR(60) NULL,
  
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentAssessmentScores PRIMARY KEY (StudentKey ASC, TimeKey ASC, AssessmentKey ASC),
  CONSTRAINT FK_FactStudentAssessmentScores_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentAssessmentScores_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentAssessmentScore_TimeKey FOREIGN KEY (AssessmentKey) REFERENCES dbo.DimAssessment(AssessmentKey)  
);


if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentCourseTranscript' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.FactStudentCourseTranscript
(
  [_sourceKey] NVARCHAR(50) NOT NULL,
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL, 
  CourseKey INT NOT NULL,
  SchoolKey INT NOT NULL,
  EarnedCredits INT NOT NULL,
  PossibleCredits INT NOT NULL,
  FinalLetterGradeEarned NVARCHAR(10)  NULL,
  FinalNumericGradeEarned DECIMAL(9,2) NULL,
    
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentCourseTranscript PRIMARY KEY (StudentKey ASC, TimeKey ASC, CourseKey ASC, SchoolKey ASC),
  CONSTRAINT FK_FactStudentCourseTranscript_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentCourseTranscript_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentCourseTranscript_CourseKey FOREIGN KEY (CourseKey) REFERENCES dbo.DimCourse(CourseKey) ,
  CONSTRAINT FK_FactStudentCourseTranscript_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey)
  
);

--Derived Tables
----------------------------------------------------------------------
--attendance by day
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'StudentAttendanceByDay' 
			   AND TABLE_SCHEMA = 'Derived')
CREATE TABLE Derived.StudentAttendanceByDay
(
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL,  
  SchoolKey INT NOT NULL,
  AttendanceEventCategoryKey INT NOT NULL ,
  [EarlyDeparture] BIT NOT NULL,
  [ExcusedAbsence] BIT NOT NULL,
  [UnexcusedAbsence] BIT NOT NULL,
  [NoContact] BIT NOT NULL,
  [InAttendance] BIT NOT NULL,
  [Tardy] BIT NOT NULL,
  
  CONSTRAINT PK_Derived_StudentAttendanceByDay PRIMARY KEY (StudentKey ASC, TimeKey ASC, SchoolKey ASC, AttendanceEventCategoryKey ASC)  
);

--ADA
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'StudentAttendanceADA' 
			   AND TABLE_SCHEMA = 'Derived')
CREATE TABLE Derived.StudentAttendanceADA
(
  StudentId NVARCHAR(32) NOT NULL, 
  StudentStateId  NVARCHAR(32)  NULL, 
  FirstName NVARCHAR(100) NOT NULL, 
  LastName NVARCHAR(100) NOT NULL,  
  DistrictSchoolCode NVARCHAR(10) NOT NULL, 
  UmbrellaSchoolCode NVARCHAR(50) NOT NULL, 	   
  SchoolName NVARCHAR(500) NOT NULL,   
  SchoolYear INT NOT NULL,	   
  NumberOfDaysPresent INT NOT NULL,
  NumberOfDaysAbsent INT NOT NULL,
  NumberOfDaysAbsentUnexcused INT NOT NULL,
  NumberOfDaysMembership INT NOT NULL,
  ADA FLOAT NOT NULL,
  
  CONSTRAINT PK_Derived_StudentAttendanceADA PRIMARY KEY (StudentId ASC, DistrictSchoolCode ASC, SchoolYear ASC)  
);


--Student Assessment Score
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'StudentAssessmentScore' 
			   AND TABLE_SCHEMA = 'Derived')
CREATE TABLE Derived.StudentAssessmentScore
(
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL, 
  AssessmentKey INT NOT NULL,
  AchievementProficiencyLevel NVARCHAR(500) NULL,
  CompositeRating  NVARCHAR(500) NULL,
  CompositeScore  NVARCHAR(500) NULL,
  PercentileRank  NVARCHAR(500) NULL,
  ProficiencyLevel  NVARCHAR(500) NULL,
  PromotionScore  NVARCHAR(500) NULL,
  RawScore  NVARCHAR(500) NULL,
  ScaleScore  NVARCHAR(500) NULL,
  
  CONSTRAINT PK_Derived_StudentAssessmentScore PRIMARY KEY (StudentKey ASC, TimeKey ASC, AssessmentKey ASC),
);

--Staff Current Schools
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'StaffCurrentSchools' 
			   AND TABLE_SCHEMA = 'Derived')

CREATE TABLE Derived.StaffCurrentSchools
(
	StaffKey INT NOT NULL,
	SchoolKey INT NOT NULL, 
	CONSTRAINT PK_Derived_StaffCurrentSchools PRIMARY KEY (StaffKey ASC, SchoolKey ASC) ,
	CONSTRAINT FK_Derived_StaffCurrentSchools_StaffKey FOREIGN KEY (StaffKey) REFERENCES dbo.DimStaff(StaffKey),
	CONSTRAINT FK_Derived_StaffCurrentSchools_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey)
);

--Staff Current Grade Levels
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'StaffCurrentGradeLevels' 
			   AND TABLE_SCHEMA = 'Derived')
			   
CREATE TABLE Derived.StaffCurrentGradeLevels
(
	Id INT NOT NULL IDENTITY(1,1),
	StaffKey INT NOT NULL,
	GradeLevel NVARCHAR(50) NOT NULL, 
	CONSTRAINT PK_Derived_StaffCurrentGradeLevels PRIMARY KEY (Id ASC) ,
	CONSTRAINT FK_Derived_StaffCurrentGradeLevels_StaffKey FOREIGN KEY (StaffKey) REFERENCES dbo.DimStaff(StaffKey),
);

--Staff Current Students
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'StaffCurrentStudents' 
			   AND TABLE_SCHEMA = 'Derived')

CREATE TABLE Derived.StaffCurrentStudents
(
	StaffKey INT NOT NULL,
	StudentKey INT NOT NULL,	
	CONSTRAINT PK_Derived_StaffCurrentStudents PRIMARY KEY (StaffKey ASC, StudentKey ASC) ,
	CONSTRAINT FK_Derived_StaffCurrentStudents_StaffKey FOREIGN KEY (StaffKey) REFERENCES dbo.DimStaff(StaffKey),
	CONSTRAINT FK_Derived_StaffCurrentStudents_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
);



	

--- Views 
----------------------------------------------------------
--assessment scores

PRINT 'creating view :  View_StudentAssessmentScores'
GO

CREATE VIEW dbo.View_StudentAssessmentScores
WITH SCHEMABINDING
AS(
   SELECT sas.StudentKey,
          sas.AssessmentKey,
		  sas.TimeKey,
		  ds.StudentUniqueId AS StudentId,
		  ds.StateId AS StudentStateId,
		  ds.FirstName,
		  ds.LastSurname AS LastName,

		  case ds.GradeLevelDescriptor_CodeValue 
		  	when 'Eighth grade' then 	'08'
		  	when 'Eleventh grade' then 	'11'
		  	when 'Fifth grade' then 	'05'
		  	when 'First grade' then 	'01'
		  	when 'Fourth grade' then 	'04'
		  	when 'Kindergarten'  then 'K'
		  	when 'Ninth grade' then 	'09'
		  	when 'Preschool/Prekindergarten' then 'PK'
		  	when 'Second grade' then 	'02'
		  	when 'Seventh grade' then 	'07'
		  	when 'Sixth grade' then 	'06'
		  	when 'Tenth grade' then 	'10'
		  	when 'Third grade' then 	'03'
		  	when 'Twelfth grade' then 	'12'
		  	ELSE ds.GradeLevelDescriptor_CodeValue 
		  end  AS GradeLevel,
		  ds.SexType_Code AS Sex,
		  ds.[SexType_Male_Indicator],
		  ds.[SexType_Female_Indicator],
		  ds.[SexType_NotSelected_Indicator],
		  
		  
		  ds.StateRaceCode AS StateRace,
		  ds.Race_AmericanIndianAlaskanNative_Indicator,
		  ds.Race_Asian_Indicator,
		  ds.Race_BlackAfricaAmerican_Indicator,
		  ds.Race_NativeHawaiianPacificIslander_Indicator,
		  ds.Race_White_Indicator,
		  ds.Race_MultiRace_Indicator,
		  ds.Race_ChooseNotRespond_Indicator,
		  ds.Race_Other_Indicator,
		  
		  ds.[EthnicityCode],
		  ds.[EthnicityHispanicLatino_Indicator],
		  ds.[Migrant_Indicator],
		  ds.Homeless_Indicator,
		  ds.IEP_Indicator,		
		  ds.[English_Learner_Code_Value] AS LEPCode,
		  ds.[English_Learner_Indicator],
		  ds.[Former_English_Learner_Indicator],
		  ds.[Never_English_Learner_Indicator],
		  ds.[EconomicDisadvantage_Indicator],

		  da.AssessmentIdentifier,
		  da.AssessmentTitle,
		  dt.SchoolDate AS AssessmentDate, 
		  
		  sas.AchievementProficiencyLevel,
		  sas.CompositeRating,
		  sas.CompositeScore,
		  sas.PercentileRank,
		  sas.ProficiencyLevel,
		  sas.PromotionScore,
		  sas.RawScore,
		  sas.ScaleScore 
FROM Derived.StudentAssessmentScore sas 
		INNER JOIN dbo.DimStudent ds ON sas.StudentKey = ds.StudentKey
		INNER JOIN dbo.DimTime dt ON sas.TimeKey = dt.TimeKey	 
		INNER JOIN dbo.DimAssessment da ON sas.AssessmentKey = da.AssessmentKey
);
GO

CREATE UNIQUE CLUSTERED INDEX CLU_View_StudentAssessmentScores
  ON dbo.View_StudentAssessmentScores (StudentKey, AssessmentKey, TimeKey)
GO

--attendance by day
PRINT 'creating view :  View_StudentAttendanceByDay'
GO

CREATE VIEW dbo.View_StudentAttendanceByDay
WITH SCHEMABINDING
AS(
    SELECT  sabd.StudentKey,
	        sabd.TimeKey,
			sabd.SchoolKey,
			sabd.AttendanceEventCategoryKey,
		    ds.StudentUniqueId AS StudentId,
			ds.StateId AS StudentStateId,
			ds.FirstName,
			ds.LastSurname AS LastName,
			case ds.GradeLevelDescriptor_CodeValue 
				when 'Eighth grade' then 	'08'
				when 'Eleventh grade' then 	'11'
				when 'Fifth grade' then 	'05'
				when 'First grade' then 	'01'
				when 'Fourth grade' then 	'04'
				when 'Kindergarten'  then 'K'
				when 'Ninth grade' then 	'09'
				when 'Preschool/Prekindergarten' then 'PK'
				when 'Second grade' then 	'02'
				when 'Seventh grade' then 	'07'
				when 'Sixth grade' then 	'06'
				when 'Tenth grade' then 	'10'
				when 'Third grade' then 	'03'
				when 'Twelfth grade' then 	'12'
				ELSE ds.GradeLevelDescriptor_CodeValue 
			end  AS GradeLevel,
			ds.SexType_Code AS Sex,
			ds.[SexType_Male_Indicator],
			ds.[SexType_Female_Indicator],
			ds.[SexType_NotSelected_Indicator],


			ds.StateRaceCode AS StateRace,
			ds.Race_AmericanIndianAlaskanNative_Indicator,
			ds.Race_Asian_Indicator,
			ds.Race_BlackAfricaAmerican_Indicator,
			ds.Race_NativeHawaiianPacificIslander_Indicator,
			ds.Race_White_Indicator,
			ds.Race_MultiRace_Indicator,
			ds.Race_ChooseNotRespond_Indicator,
			ds.Race_Other_Indicator,

			ds.[EthnicityCode],
			ds.[EthnicityHispanicLatino_Indicator],
			ds.[Migrant_Indicator],
			ds.Homeless_Indicator,
			ds.IEP_Indicator,		
			ds.[English_Learner_Code_Value] AS LEPCode,
			ds.[English_Learner_Indicator],
			ds.[Former_English_Learner_Indicator],
			ds.[Never_English_Learner_Indicator],
			ds.[EconomicDisadvantage_Indicator],

			dsc.DistrictSchoolCode AS DistrictSchoolCode,
		    dsc.UmbrellaSchoolCode AS UmbrellaSchoolCode,
			dsc.NameOfInstitution AS SchoolName,			
			dt.SchoolDate AS AttedanceDate, 		
			dt.SchoolYear,
			sabd.[EarlyDeparture],
			sabd.[ExcusedAbsence],
			sabd.[UnexcusedAbsence],
			sabd.[NoContact],
			sabd.[InAttendance]
	FROM Derived.[StudentAttendanceByDay] sabd 
			INNER JOIN dbo.DimStudent ds ON sabd.StudentKey = ds.StudentKey
			INNER JOIN dbo.DimTime dt ON sabd.TimeKey = dt.TimeKey	 
			INNER JOIN dbo.DimSchool dsc ON sabd.SchoolKey = dsc.SchoolKey	 			 
	WHERE 1=1 
	--AND ds.StudentUniqueId = 341888
	--AND dt.SchoolDate = '2018-10-26'
);
GO

CREATE UNIQUE CLUSTERED INDEX CLU_View_StudentAttendanceByDay
  ON dbo.View_StudentAttendanceByDay (StudentKey,SchoolKey, TimeKey, AttendanceEventCategoryKey)
GO


--attendance by day
PRINT 'creating view :  View_StudentAttendance_ADA'
GO

CREATE VIEW dbo.View_StudentAttendance_ADA
WITH SCHEMABINDING
AS (
     select  StudentId, 			 
			 FirstName, 
			 LastName, 
			 [DistrictSchoolCode],
			 [UmbrellaSchoolCode],	   
			 SchoolName, 	   
			 SchoolYear,
			 [NumberOfDaysPresent]
			,[NumberOfDaysAbsent]
			,[NumberOfDaysAbsentUnexcused]
			,[NumberOfDaysMembership]
			,[ADA]
	 FROM [Derived].[StudentAttendanceADA]
	 
);
GO
CREATE UNIQUE CLUSTERED INDEX CLU_View_StudentAttendance_ADA
  ON dbo.View_StudentAttendance_ADA (StudentId,DistrictSchoolCode, SchoolYear)
GO


--behavior incidents
PRINT 'creating view :  View_StudentDiscipline'
GO
CREATE VIEW dbo.View_StudentDiscipline
WITH SCHEMABINDING
AS(
SELECT  fsd.StudentKey,
        fsd.TimeKey,
		fsd.SchoolKey,
		fsd.DisciplineIncidentKey,
		ds.StudentUniqueId AS StudentId,
		ds.StateId AS StudentStateId,
		ds.FirstName,
		ds.LastSurname AS LastName,
		ds.StateRaceCode as RaceCode,		
		case ds.GradeLevelDescriptor_CodeValue 
			when 'Eighth grade' then 	'08'
			when 'Eleventh grade' then 	'11'
			when 'Fifth grade' then 	'05'
			when 'First grade' then 	'01'
			when 'Fourth grade' then 	'04'
			when 'Kindergarten'  then 'K'
			when 'Ninth grade' then 	'09'
			when 'Preschool/Prekindergarten' then 'PK'
			when 'Second grade' then 	'02'
			when 'Seventh grade' then 	'07'
			when 'Sixth grade' then 	'06'
			when 'Tenth grade' then 	'10'
			when 'Third grade' then 	'03'
			when 'Twelfth grade' then 	'12'
			ELSE ds.GradeLevelDescriptor_CodeValue 
		end  AS GradeLevel,
		ds.SexType_Code AS Sex,
		ds.[SexType_Male_Indicator],
		ds.[SexType_Female_Indicator],
		ds.[SexType_NotSelected_Indicator],


		ds.StateRaceCode AS StateRace,
		ds.Race_AmericanIndianAlaskanNative_Indicator,
		ds.Race_Asian_Indicator,
		ds.Race_BlackAfricaAmerican_Indicator,
		ds.Race_NativeHawaiianPacificIslander_Indicator,
		ds.Race_White_Indicator,
		ds.Race_MultiRace_Indicator,
		ds.Race_ChooseNotRespond_Indicator,
		ds.Race_Other_Indicator,

		ds.[EthnicityCode],
		ds.[EthnicityHispanicLatino_Indicator],
		ds.[Migrant_Indicator],
		ds.Homeless_Indicator,
		ds.IEP_Indicator,		
		ds.[English_Learner_Code_Value] AS LEPCode,
		ds.[English_Learner_Indicator],
		ds.[Former_English_Learner_Indicator],
		ds.[Never_English_Learner_Indicator],
		ds.[EconomicDisadvantage_Indicator],

		dsc.DistrictSchoolCode AS DistrictSchoolCode,
		dsc.UmbrellaSchoolCode AS UmbrellaSchoolCode,
		dsc.ShortNameOfInstitution AS SchoolName,
		dt.SchoolDate AS IncidentDate, 	
		dt.SchoolYear AS IncidentSchoolYear,
		dt.SchoolTermDescriptor_CodeValue AS SchoolTerm,
		ddi._sourceKey AS IncidentIdentifier,
		ddi.IncidentTime,
		ddi.IncidentDescription,
		ddi.BehaviorDescriptor_CodeValue AS IncidentType,
		ddi.LocationDescriptor_CodeValue AS IncidentLocation,
		ddi.DisciplineDescriptor_CodeValue AS IncidentAction ,
		ddi.ReporterDescriptor_CodeValue AS IncidentReporter,
		ddi.DisciplineDescriptor_ISS_Indicator AS IsISS,
		ddi.DisciplineDescriptor_OSS_Indicator AS IsOSS		
FROM dbo.FactStudentDiscipline fsd 
		INNER JOIN dbo.DimStudent ds ON fsd.StudentKey = ds.StudentKey
		INNER JOIN dbo.DimTime dt ON fsd.TimeKey = dt.TimeKey	 
		INNER JOIN dbo.DimSchool dsc ON fsd.SchoolKey = dsc.SchoolKey	 
		INNER JOIN dbo.DimDisciplineIncident ddi ON fsd.DisciplineIncidentKey = ddi.DisciplineIncidentKey		
);
GO

CREATE UNIQUE CLUSTERED INDEX CLU_View_StudentDiscipline
  ON dbo.View_StudentDiscipline (StudentKey, TimeKey, SchoolKey, DisciplineIncidentKey)
GO


--behavior incidents
PRINT 'creating view :  View_StudentCourseTranscript'
GO
CREATE VIEW dbo.View_StudentCourseTranscript
WITH SCHEMABINDING
AS(
SELECT  fsct.StudentKey,
        fsct.TimeKey,
		fsct.SchoolKey,
		fsct.CourseKey,
		ds.StudentUniqueId AS StudentId,
		ds.StateId AS StudentStateId,
		ds.FirstName,
		ds.LastSurname AS LastName,		
		case ds.GradeLevelDescriptor_CodeValue 
			when 'Eighth grade' then 	'08'
			when 'Eleventh grade' then 	'11'
			when 'Fifth grade' then 	'05'
			when 'First grade' then 	'01'
			when 'Fourth grade' then 	'04'
			when 'Kindergarten'  then 'K'
			when 'Ninth grade' then 	'09'
			when 'Preschool/Prekindergarten' then 'PK'
			when 'Second grade' then 	'02'
			when 'Seventh grade' then 	'07'
			when 'Sixth grade' then 	'06'
			when 'Tenth grade' then 	'10'
			when 'Third grade' then 	'03'
			when 'Twelfth grade' then 	'12'
			ELSE ds.GradeLevelDescriptor_CodeValue 
		end  AS GradeLevel,
		ds.SexType_Code AS Sex,
		ds.[SexType_Male_Indicator],
		ds.[SexType_Female_Indicator],
		ds.[SexType_NotSelected_Indicator],


		ds.StateRaceCode AS StateRace,
		ds.Race_AmericanIndianAlaskanNative_Indicator,
		ds.Race_Asian_Indicator,
		ds.Race_BlackAfricaAmerican_Indicator,
		ds.Race_NativeHawaiianPacificIslander_Indicator,
		ds.Race_White_Indicator,
		ds.Race_MultiRace_Indicator,
		ds.Race_ChooseNotRespond_Indicator,
		ds.Race_Other_Indicator,

		ds.[EthnicityCode],
		ds.[EthnicityHispanicLatino_Indicator],
		ds.[Migrant_Indicator],
		ds.Homeless_Indicator,
		ds.IEP_Indicator,		
		ds.[English_Learner_Code_Value] AS LEPCode,
		ds.[English_Learner_Indicator],
		ds.[Former_English_Learner_Indicator],
		ds.[Never_English_Learner_Indicator],
		ds.[EconomicDisadvantage_Indicator],

		dsc.DistrictSchoolCode AS DistrictSchoolCode,
		dsc.UmbrellaSchoolCode AS UmbrellaSchoolCode,
		dsc.NameOfInstitution AS SchoolName,
		dc.CourseCode,
		dc.CourseTitle,
		dc.CourseLevelCharacteristicTypeDescriptor_CodeValue AS CourseType,
		dc.SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue AS MassCore,
		dt.SchoolTermDescriptor_CodeValue AS Term, 		
		fsct.EarnedCredits,
		fsct.PossibleCredits,
		fsct.FinalLetterGradeEarned,
		fsct.FinalNumericGradeEarned
FROM dbo.FactStudentCourseTranscript fsct
		INNER JOIN dbo.DimStudent ds ON fsct.StudentKey = ds.StudentKey
		INNER JOIN dbo.DimTime dt ON fsct.TimeKey = dt.TimeKey	 
		INNER JOIN dbo.DimSchool dsc ON fsct.SchoolKey = dsc.SchoolKey		
		INNER JOIN dbo.DimCourse dc ON fsct.CourseKey = dc.CourseKey		
);

GO
CREATE UNIQUE CLUSTERED INDEX CLU_View_StudentCourseTranscript
  ON dbo.View_StudentCourseTranscript (StudentKey, TimeKey, SchoolKey, CourseKey)
GO

PRINT 'creating view :  View_StudentRoster'
GO
CREATE VIEW dbo.View_StudentRoster
WITH SCHEMABINDING
AS(
SELECT  
		ds.StudentKey AS StudentKey,
		ds.StudentUniqueId AS StudentId,
		ds.StateId AS StudentStateId,
		ds.FirstName,
		ds.MiddleName,
		ds.MiddleInitial,
		ds.FullName,
		ds.LastSurname AS LastName,
		ds.PrimaryElectronicMailAddress AS StudentEmail,
		ds.GradeLevelDescriptor_CodeValue AS SourceGradeLevel ,
		case ds.GradeLevelDescriptor_CodeValue 
			when 'Eighth grade' then 	'08'
			when 'Eleventh grade' then 	'11'
			when 'Fifth grade' then 	'05'
			when 'First grade' then 	'01'
			when 'Fourth grade' then 	'04'
			when 'Kindergarten'  then 'K'
			when 'Ninth grade' then 	'09'
			when 'Preschool/Prekindergarten' then 'PK'
			when 'Second grade' then 	'02'
			when 'Seventh grade' then 	'07'
			when 'Sixth grade' then 	'06'
			when 'Tenth grade' then 	'10'
			when 'Third grade' then 	'03'
			when 'Twelfth grade' then 	'12'
			ELSE ds.GradeLevelDescriptor_CodeValue 
		end  AS GradeLevel,
		ds.BirthDate,
		ds.StudentAge,
		ds.[GraduationSchoolYear],
		dsc.DistrictSchoolCode AS DistrictSchoolCode,
		dsc.StateSchoolCode AS StateSchoolCode,
		dsc.UmbrellaSchoolCode AS SchoolUmbrellaCode,
		dsc.NameOfInstitution AS SchoolName,
		
		ds.Homeroom,
		ds.HomeroomTeacher,
		ds.SexType_Code AS Sex,
		ds.[SexType_Male_Indicator],
	    ds.[SexType_Female_Indicator],
	    ds.[SexType_NotSelected_Indicator],


		ds.StateRaceCode AS StateRace,
		ds.Race_AmericanIndianAlaskanNative_Indicator,
		ds.Race_Asian_Indicator,
		ds.Race_BlackAfricaAmerican_Indicator,
		ds.Race_NativeHawaiianPacificIslander_Indicator,
		ds.Race_White_Indicator,
		ds.Race_MultiRace_Indicator,
		ds.Race_ChooseNotRespond_Indicator,
		ds.Race_Other_Indicator,

		ds.[EthnicityCode],
		ds.[EthnicityHispanicLatino_Indicator],
		ds.[Migrant_Indicator],
		ds.Homeless_Indicator,
		ds.IEP_Indicator,		
		ds.[English_Learner_Code_Value] AS LEPCode,
		ds.[English_Learner_Indicator],
		ds.[Former_English_Learner_Indicator],
		ds.[Never_English_Learner_Indicator],
		ds.[EconomicDisadvantage_Indicator],

		ds.EntryDate,
		ds.EntrySchoolYear,
		ds.EntryCode,

		ds.ExitWithdrawDate,
		ds.ExitWithdrawSchoolYear,
		ds.ExitWithdrawCode,

		ds.ValidFrom,
		ds.ValidTo,
		ds.IsCurrent
		
		
FROM dbo.DimStudent ds 		
     INNER JOIN dbo.DimSchool dsc ON ds.SchoolKey = dsc.SchoolKey
		
);

GO
CREATE UNIQUE CLUSTERED INDEX CLU_View_StudentRoster
  ON dbo.View_StudentRoster (StudentKey)
GO


--ETL Related Objects
------------------------------------------------------------------------------

--functions
--------------------------------------------------------------
--create function to derive schoolyear from a date
CREATE FUNCTION dbo.Func_ETL_GetSchoolYear
(
    @CurrentDate DATETIME
)
RETURNS INT
WITH SCHEMABINDING
AS
BEGIN

    -- Declare the return variable here
    DECLARE @Result INT;

    DECLARE @schoolYearRolloverDate DATE = '07/01/9999';


    IF (DATEPART(DAYOFYEAR, @CurrentDate) >= DATEPART(DAYOFYEAR, @schoolYearRolloverDate))
    BEGIN
        SET @Result = YEAR(@CurrentDate) + 1;
    END;
    ELSE
    BEGIN
        SET @Result = YEAR(@CurrentDate);
    END;

    -- Return the result of the function
    RETURN @Result;

END;
GO
--create function to derive eastern holidays from a date
CREATE FUNCTION dbo.Func_ETL_GetEasterHolidays
(
    @TheYear INT
)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
(
    WITH x
    AS (SELECT TheDate = DATEFROMPARTS(@TheYear, [Month], [Day])
        FROM
        (
            SELECT [Month],
                   [Day] = DaysToSunday + 28 - (31 * ([Month] / 4))
            FROM
            (
                SELECT [Month] = 3 + (DaysToSunday + 40) / 44,
                       DaysToSunday
                FROM
                (
                    SELECT DaysToSunday = paschal - ((@TheYear + (@TheYear / 4) + paschal - 13) % 7)
                    FROM
                    (
                        SELECT paschal = epact - (epact / 28)
                        FROM
                        (SELECT epact = (24 + 19 * (@TheYear % 19)) % 30) AS epact
                    ) AS paschal
                ) AS dts
            ) AS m
        ) AS d )
    SELECT TheDate,
           HolidayText = 'Easter Sunday'
    FROM x
    UNION ALL
    SELECT DATEADD(DAY, -2, TheDate),
           'Good Friday'
    FROM x
    UNION ALL
    SELECT DATEADD(DAY, 1, TheDate),
           'Easter Monday'
    FROM x
);
GO

--create function to derive holidays from a date
CREATE FUNCTION [dbo].[Func_ETL_GetHolidayFromDate]
(
    @date  date
)
RETURNS NVARCHAR(200)

AS
BEGIN
    DECLARE @holidayName NVARCHAR(200) = 'Non-Holiday';
    -- for ease of typing
    DECLARE @year  int = DATEPART(YEAR, @date);
    DECLARE @month int = DATEPART(MONTH,@date);
    DECLARE @day   int = DATEPART(DAY, @date);
    DECLARE @dayName varchar(12) = DATENAME(DW, @date );

    DECLARE @nthWeekDay int = ceiling(@day / 7.0);
    DECLARE @isThursday bit = CASE WHEN @dayName LIKE 'Thursday' THEN 1 ELSE 0 END;
    DECLARE @isFriday   bit = CASE WHEN @dayName LIKE 'Friday' THEN 1 ELSE 0 END;
    DECLARE @isSaturday bit = CASE WHEN @dayName LIKE 'Saturday' THEN 1 ELSE 0 END;
    DECLARE @isSunday   bit = CASE WHEN @dayName LIKE 'Sunday' THEN 1 ELSE 0 END;
    DECLARE @isMonday   bit = CASE WHEN @dayName LIKE 'Monday' THEN 1 ELSE 0 END;
    DECLARE @isWeekend  bit = CASE WHEN @isSaturday = 1 OR @isSunday = 1 THEN 1 ELSE 0 END;
     
    ---- New Years Day
    if ((@month = 12 AND @day = 31 AND @isFriday=1)  
	OR (@month = 1 AND @day = 1 AND @isWeekend=0) 
    OR (@month = 1 AND @day = 2 AND @isMonday=1))
	  BEGIN
	     SET @holidayName = 'New Years Day';
      END
	
    ---- MLK day
    if (@month = 1 AND @isMonday = 1 AND @nthWeekDay = 3)  
	  BEGIN
	     SET @holidayName = 'MLK day';
      END

    ------ President’s Day ( 3rd Monday in February )
    if (@month = 2 AND @isMonday = 1 AND @nthWeekDay = 3) 
	  BEGIN
	     SET @holidayName = 'President’s Day';
      END

    ------ Memorial Day ( Last Monday in May )
    if (@month = 5 AND @isMonday = 1 AND DATEPART(MONTH, DATEADD(DAY, 7, @Date)) = 6)  
	  BEGIN
	     SET @holidayName = 'Memorial Day';
      END

	------ Independence Day ( July 4 )
	if ((@month = 7 AND @day = 3 AND @isFriday = 1)
	OR (@month = 7 AND @day = 4 AND @isWeekend = 0)
	OR (@month = 7 AND @day = 5 AND @isMonday = 1) )
	  BEGIN
	     SET @holidayName = 'Independence Day';
      END

    ------ Labor Day ( 1st Monday in September )
    if (@month = 9 AND @isMonday = 1 AND @nthWeekDay = 1) 
	  BEGIN
	     SET @holidayName = 'Labor Day';
      END

    ------ Columbus Day ( 2nd Monday in October )
    if (@month = 10 AND @isMonday = 1 AND @nthWeekDay = 2) 
	  BEGIN
	     SET @holidayName = 'Labor Day';
      END

    ------ Veteran’s Day ( November 11 )
	if ((@month = 11 AND @day = 10 AND @isFriday = 1)
	OR (@month = 11 AND @day = 11 AND @isWeekend = 0)
	OR (@month = 11 AND @day = 12 AND @isMonday = 1))
	  BEGIN
	     SET @holidayName = 'Veteran’s Day';
      END

    ------ Thanksgiving Day ( 4th Thursday in November )
    if (@month = 11 AND @isThursday = 1 AND @nthWeekDay = 4) 
	  BEGIN
	     SET @holidayName = 'Thanksgiving Day';
      END

    ------ Christmas Day ( December 25 )
    if ((@month = 12 AND @day = 24 AND @isFriday = 1) 
    or (@month = 12 AND @day = 25 AND @isWeekend = 0)
    or (@month = 12 AND @day = 25 AND @isMonday = 1))
	  BEGIN
	     SET @holidayName = 'Christmas Day';
      END

    RETURN @holidayName;
	
END

GO

--create function to derive schoolyear from a date
CREATE FUNCTION dbo.Func_ETL_GetFullName
(
    @fName NVARCHAR(256),
    @mName NVARCHAR(256),
    @lName NVARCHAR(256)
)
RETURNS NVARCHAR(768)
AS
BEGIN
    DECLARE @fullName NVARCHAR(768);
    SELECT @fullName   = CONCAT_WS(' ',RTRIM(LTRIM(@fName)), LTRIM(COALESCE(@mName, '')), RTRIM(LTRIM(@lName)));
    RETURN @fullName;
END;

GO

--tables 
-------------------------------------------------------------

--ETL_Lineage
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'ETL_Lineage' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.ETL_Lineage
(
   LineageKey INT NOT NULL IDENTITY(1,1), -- surrogate
   TableName NVARCHAR(100) NOT NULL,
   StartTime DATETIME NOT NULL,
   EndTime DATETIME NULL,
   LoadType CHAR(1) NOT NULL , -- F = Full, I = Incremental
   [Status] CHAR(1) NOT NULL, -- P = Processing , S = Success
   CONSTRAINT PK_ETL_Lineage PRIMARY KEY (LineageKey),
);
GO

--ETL_IncrementalLoads
IF NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'ETL_IncrementalLoads' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE [dbo].[ETL_IncrementalLoads](
	[LoadDateKey] [int] IDENTITY(1,1) NOT NULL,
	[TableName] [nvarchar](100) NOT NULL,
	[LoadDate] [datetime] NOT NULL,
 CONSTRAINT [PK_LoadDates] PRIMARY KEY CLUSTERED  ([LoadDateKey] ASC)
) ON [PRIMARY]

GO

--school
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'School' 
			   AND TABLE_SCHEMA = 'Staging')
CREATE TABLE Staging.School
(
  SchoolKey int NOT NULL IDENTITY(1,1),  -- surrogate
  [_sourceKey] NVARCHAR(50) NOT NULL,  --'Ed-Fi|Id'
  DistrictSchoolCode NVARCHAR(10) NULL ,
  StateSchoolCode NVARCHAR(50) NULL ,
  UmbrellaSchoolCode NVARCHAR(50) NULL,

  ShortNameOfInstitution NVARCHAR(500) NOT NULL,     
  NameOfInstitution NVARCHAR(500) NOT NULL,    

  SchoolCategoryType NVARCHAR(100) NOT NULL,     -- elem, middle, hs, combined
  SchoolCategoryType_Elementary_Indicator BIT NOT NULL,      
  SchoolCategoryType_Middle_Indicator BIT NOT NULL,
  SchoolCategoryType_HighSchool_Indicator BIT NOT NULL,    
  SchoolCategoryType_Combined_Indicator BIT NOT NULL,    
  SchoolCategoryType_Other_Indicator BIT NOT NULL,    
  
  TitleIPartASchoolDesignationTypeCodeValue  NVARCHAR(50) NOT NULL,--Not designated as a Title I Part A school
																	--Title I Part A Schoolwide Assistance Program Schoo
																	--Title I Part A Targeted Assistance School
																	--Title I targeted eligible school - no program
																	--Title I targeted school
																	--Title I school wide eligible - Title I targeted pr
																	--Title I school wide eligible school - no program
                                            
  TitleIPartASchoolDesignation_Indicator BIT NOT NULL, -- True,False
  OperationalStatusTypeDescriptor_CodeValue NVARCHAR(50) NOT NULL,
  OperationalStatusTypeDescriptor_Description NVARCHAR(1024) NOT NULL,
  
  SchoolNameModifiedDate [datetime] NOT NULL,
  SchoolOperationalStatusTypeModifiedDate [DATETIME] NOT NULL,
  SchoolCategoryModifiedDate [datetime] NOT NULL,
  SchoolTitle1StatusModifiedDate [datetime] NOT NULL,

  ValidFrom DATETIME NOT NULL,
  ValidTo DATETIME NOT NULL,
  IsCurrent BIT NOT NULL,  
  
  CONSTRAINT PK_StagingSchool PRIMARY KEY (SchoolKey),  
);
GO

--time
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'Time' 
			   AND TABLE_SCHEMA = 'Staging')
CREATE TABLE Staging.[Time]
(
  TimeKey INT NOT NULL IDENTITY(1,1), -- ex 9/1/2019 : 20190901 -- surrogate    
  
  SchoolDate DATE NOT NULL , -- 9/1/2019
  SchoolDate_MMYYYY CHAR(6) NOT NULL,
  SchoolDate_Fomat1 CHAR(10) NOT NULL,
  SchoolDate_Fomat2 CHAR(8) NOT NULL,
  SchoolDate_Fomat3 CHAR(10) NOT NULL,
    
  SchoolYear SMALLINT NOT NULL, -- ex: 9/1/2019 = 2020  
  SchoolYearDescription NVARCHAR(50) NOT NULL, -- '2019-2020 or SchoolYear 2019 - 2020'
  CalendarYear SMALLINT NOT NULL, -- ex: 9/1/2019 = 2020
  
  DayOfMonth TINYINT NOT NULL, -- 1 - 30|31
  DaySuffix CHAR(2) NOT NULL , -- 1st, 2nd, 3rd
  DayName  NVARCHAR(15) NOT NULL, -- Monday, Tuesday    
  DayNameShort NVARCHAR(15) NOT NULL, -- Mon, Tue
  DayOfWeek TINYINT NOT NULL, -- 1 - 7
  
  WeekInMonth TINYINT NOT NULL, -- 1 - 4 or 5 -- this counts 7 days starting on the 1st of the month
  WeekOfMonth TINYINT NOT NULL, -- 1 - 4 or 5 -- this is the actual week of the month (starting on sunday)
  Weekend_Indicator BIT NOT NULL,
  WeekOfYear TINYINT NOT NULL, -- 1 - 53   
  FirstDayOfWeek DATE NOT NULL,
  LastDayOfWeek DATE NOT NULL,
  WeekBeforeChristmas_Indicator BIT NOT NULL, --  True,False
  
  
  [Month] TINYINT NOT NULL, -- 1..12
  MonthName  NVARCHAR(10) NOT NULL, --January,February,December
  MonthNameShort CHAR(3) NOT NULL, --Jan,Feb,Dec  
  FirstDayOfMonth DATE NOT NULL,
  LastDayOfMonth DATE NOT NULL,
  FirstDayOfNextMonth DATE NOT NULL,
  LastDayOfNextMonth DATE NOT NULL,
    
  DayOfYear SMALLINT NULL, -- 1 - 365 or 366 (Leap Year Every Four Years)  
  
  LeapYear_Indicator BIT NOT NULL,  
    
  FederalHolidayName NVARCHAR(20) NULL, -- Memorial Day
  FederalHoliday_Indicator BIT NOT NULL, --  True,False
  
  --all these vary by school
  SchoolSourceKey NVARCHAR(50) NULL,  
  DayOfSchoolYear SMALLINT NULL, -- 1 - 180 - based on SIS(ODS) school calendar
  SchoolCalendarEventType_CodeValue NVARCHAR(50) NULL, -- Emergency day,Instructional day,Teacher only day
  SchoolCalendarEventType_Description NVARCHAR(50) NULL, -- Emergency day,Instructional day,Teacher only day
    
  SchoolTermDescriptor_CodeValue NVARCHAR(50) NULL, -- Year Round,First Quarter, First Trimester, Fall Semester, Fourth Quarter, etc.  SELECT * FROM v25_EdFi_Ods_Sandbox_populatedSandbox.edfi.Descriptor where namespace = 'http://ed-fi.org/Descriptor/TermDescriptor.xml'
  SchoolTermDescriptor_Description NVARCHAR(50) NULL, -- Year Round,First Quarter, First Trimester, Fall Semester, Fourth Quarter, etc SELECT * FROM v25_EdFi_Ods_Sandbox_populatedSandbox.edfi.Descriptor where namespace = 'http://ed-fi.org/Descriptor/TermDescriptor.xml'

  
  SchoolSessisonModifiedDate [datetime] NOT NULL,
  CalendarEventTypeModifiedDate [DATETIME] NOT NULL,
      
  ValidFrom DATETIME NOT NULL,
  ValidTo DATETIME NOT NULL,
  IsCurrent BIT NOT NULL
  CONSTRAINT PK_StagingTime PRIMARY KEY (TimeKey)  
);
GO

--staff
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'Staff' 
			   AND TABLE_SCHEMA = 'Staging')
CREATE TABLE Staging.Staff
(
  StaffKey INT NOT NULL IDENTITY(1,1),
  [_sourceKey] NVARCHAR(50) NOT NULL,  --'EdFi|StaffUSI'
  
  PrimaryElectronicMailAddress [nvarchar](128) NULL,
  PrimaryElectronicMailTypeDescriptor_CodeValue [nvarchar](128) NULL, -- Home/Personal, Organization, Other, Work
  PrimaryElectronicMailTypeDescriptor_Description [nvarchar](128) NULL,
    
  [StaffUniqueId] [nvarchar](32) NOT NULL,
  [PersonalTitlePrefix] [nvarchar](30) NULL,
  [FirstName] [nvarchar](75) NOT NULL,
  [MiddleName] [nvarchar](75) NULL,
  [MiddleInitial] CHAR(1) NULL,
  [LastSurname] nvarchar(75) NOT NULL,
  [FullName] NVARCHAR(50) NOT NULL,
  [GenerationCodeSuffix] [nvarchar](10) NULL,
  [MaidenName] [nvarchar](75) NULL,  
  [BirthDate] DATE NULL,
  [StaffAge] INT NULL,  
  
  SexType_Code NVARCHAR(15) NOT NULL,
  SexType_Description NVARCHAR(100) NOT NULL,    
  SexType_Male_Indicator BIT NOT NULL,
  SexType_Female_Indicator BIT NOT NULL,
  SexType_NotSelected_Indicator BIT NOT NULL,
     
  [HighestLevelOfEducationDescriptorDescriptor_CodeValue] NVARCHAR(100)  NULL, 
  [HighestLevelOfEducationDescriptorDescriptor_Description] NVARCHAR(100)  NULL, 
  [YearsOfPriorProfessionalExperience] [decimal](5, 2) NULL,
  [YearsOfPriorTeachingExperience] [decimal](5, 2) NULL,  
  [HighlyQualifiedTeacher_Indicator] [bit] NULL,
    
  [StaffClassificationDescriptor_CodeValue]  NVARCHAR(100)  NULL, 
  [StaffClassificationDescriptor_CodeDescription]  NVARCHAR(100)  NULL, 
  
  StaffMainInfoModifiedDate [datetime] NOT NULL,  

  ValidFrom DATETIME NOT NULL, 
  ValidTo DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL  

  CONSTRAINT PK_DimStaff PRIMARY KEY (StaffKey)   
);
GO


--student
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'Student' 
			   AND TABLE_SCHEMA = 'Staging')
CREATE TABLE Staging.Student
(
    [StudentKey] [int] IDENTITY(1,1) NOT NULL, 
	[_sourceKey] [nvarchar](50) NOT NULL,

	[StudentUniqueId] [nvarchar](32) NULL,
	[StateId] [nvarchar](32) NULL,

	PrimaryElectronicMailAddress [nvarchar](128) NULL,
	PrimaryElectronicMailTypeDescriptor_CodeValue [nvarchar](128) NULL, -- Home/Personal, Organization, Other, Work
	PrimaryElectronicMailTypeDescriptor_Description [nvarchar](128) NULL,

	[SchoolKey] [int] NOT NULL,
	[ShortNameOfInstitution] [nvarchar](500) NOT NULL,
	[NameOfInstitution] [nvarchar](500) NOT NULL,
	[GradeLevelDescriptor_CodeValue] [nvarchar](100) NOT NULL,
	[GradeLevelDescriptor_Description] [nvarchar](500) NOT NULL,	
	
	[FirstName] [nvarchar](100) NOT NULL,
	[MiddleInitial] [char](1) NULL,
	[MiddleName] [nvarchar](100) NULL,
	[LastSurname] [nvarchar](100) NOT NULL,
	[FullName] [nvarchar](500) NOT NULL,
	[BirthDate] [date] NOT NULL,
	[StudentAge] [int] NOT NULL,
	[GraduationSchoolYear] [int] NULL,
	
	[Homeroom] [nvarchar](500) NULL,
	[HomeroomTeacher] [nvarchar](500) NULL,

	[SexType_Code] [nvarchar](100) NOT NULL,
	[SexType_Description] [nvarchar](100) NOT NULL,
	[SexType_Male_Indicator] [bit] NOT NULL,
	[SexType_Female_Indicator] [bit] NOT NULL,
	[SexType_NotSelected_Indicator] [bit] NOT NULL,
	
	[RaceCode] [nvarchar](1000) NOT NULL,
	[RaceDescription] [nvarchar](1000) NOT NULL,
	[StateRaceCode] [nvarchar](1000) NOT NULL,
	[Race_AmericanIndianAlaskanNative_Indicator] [bit] NOT NULL,
	[Race_Asian_Indicator] [bit] NOT NULL,
	[Race_BlackAfricaAmerican_Indicator] [bit] NOT NULL,
	[Race_NativeHawaiianPacificIslander_Indicator] [bit] NOT NULL,
	[Race_White_Indicator] [bit] NOT NULL,
	[Race_MultiRace_Indicator] [bit] NOT NULL,
	[Race_ChooseNotRespond_Indicator] [bit] NOT NULL,
	[Race_Other_Indicator] [bit] NOT NULL,

	[EthnicityCode] [nvarchar](100) NOT NULL,
	[EthnicityDescription] [nvarchar](100) NOT NULL,
	[EthnicityHispanicLatino_Indicator] [bit] NOT NULL,
	[Migrant_Indicator] [bit] NOT NULL,
	[Homeless_Indicator] [bit] NOT NULL,
	[IEP_Indicator] [bit] NOT NULL,
	[English_Learner_Code_Value] [nvarchar](100) NOT NULL,
	[English_Learner_Description] [nvarchar](100) NOT NULL,
	[English_Learner_Indicator] [bit] NOT NULL,
	[Former_English_Learner_Indicator] [bit] NOT NULL,
	[Never_English_Learner_Indicator] [bit] NOT NULL,
	[EconomicDisadvantage_Indicator] [bit] NOT NULL,
	
	[EntryDate] [datetime2](7) NOT NULL,
	[EntrySchoolYear] [int] NOT NULL,
	[EntryCode] [nvarchar](25) NOT NULL,
	
	[ExitWithdrawDate] [datetime2](7) NULL,
	[ExitWithdrawSchoolYear] [int] NULL,
	[ExitWithdrawCode] [nvarchar](100) NULL,

	StudentMainInfoModifiedDate [datetime] NOT NULL,
	StudentSchoolAssociationModifiedDate [datetime] NOT NULL,

	[ValidFrom] [datetime] NOT NULL,
	[ValidTo] [datetime] NOT NULL,
	[IsCurrent] [bit] NOT NULL
	

    CONSTRAINT PK_StagingStudent PRIMARY KEY (StudentKey)    
);
GO

--attendance event category
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'AttendanceEventCategory' 
			   AND TABLE_SCHEMA = 'Staging')
CREATE TABLE Staging.AttendanceEventCategory
(
  AttendanceEventCategoryKey INT NOT NULL IDENTITY(1,1),
  [_sourceKey] NVARCHAR(50) NOT NULL, 
   
  AttendanceEventCategoryDescriptor_CodeValue nvarchar(50) NOT NULL,
  AttendanceEventCategoryDescriptor_Description nvarchar(1024) NOT NULL,
  
  [InAttendance_Indicator] BIT NOT NULL,  
  [UnexcusedAbsence_Indicator] BIT NOT NULL,
  [ExcusedAbsence_Indicator] BIT NOT NULL,  
  [Tardy_Indicator] BIT NOT NULL,    
  [EarlyDeparture_Indicator]  BIT NOT NULL,    
  
  CategoryModifiedDate [datetime] NOT NULL,

  ValidFrom DATETIME NOT NULL, 
  ValidTo DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL,   
  CONSTRAINT PK_stagingAttendanceEventCategory PRIMARY KEY (AttendanceEventCategoryKey ASC)  
);
GO

if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DisciplineIncident' 
			   AND TABLE_SCHEMA = 'Staging')
CREATE TABLE Staging.DisciplineIncident
(
  DisciplineIncidentKey INT NOT NULL IDENTITY(1,1),
  [_sourceKey] NVARCHAR(50) NOT NULL, 
  
  SchoolKey INT NOT NULL,
  ShortNameOfInstitution nvarchar(500) NOT NULL,
  NameOfInstitution nvarchar(500) NOT NULL,
  SchoolYear INT NOT NULL,
  IncidentDate DATE NOT NULL,   
  IncidentTime TIME(7) NOT NULL,   
  [IncidentDescription] nvarchar(MAX) NULL ,
  [BehaviorDescriptor_CodeValue] nvarchar(50) not null, -- IncidentType: Weapons Possession (Firearms and Other Weapons), Drugs, Abuse Of Volatile Chemical,School Code of Conduct,  etc
  [BehaviorDescriptor_Description] nvarchar(1024) not null,

  [LocationDescriptor_CodeValue] nvarchar(50) not null,  -- Hallway, Cafeteria, Classroom, etc
  [LocationDescriptor_Description] nvarchar(1024) not null,

  [DisciplineDescriptor_CodeValue] nvarchar(50) not null, -- Actions: Community Service, Expulsion,In School Suspension,Out of School Suspension, Removal from Classroom, etc
  [DisciplineDescriptor_Description] nvarchar(1024) not null,
  DisciplineDescriptor_ISS_Indicator BIT NOT NULL,
  DisciplineDescriptor_OSS_Indicator BIT NOT NULL,

  ReporterDescriptor_CodeValue nvarchar(50) NOT NULL, -- Law enforcement officer,Non-school personnel,Other,Parent/guardian,Staff,Student  
  ReporterDescriptor_Description nvarchar(1024) NOT NULL,
  
  IncidentReporterName NVARCHAR(100) NOT NULL ,
  ReportedToLawEnforcement_Indicator BIT NOT NULL ,
  IncidentCost Money NOT NULL,
  
  IncidentModifiedDate [datetime] NOT NULL,

  ValidFrom DATETIME NOT NULL,
  ValidTo DATETIME NOT NULL,
  IsCurrent BIT NOT NULL,  

  CONSTRAINT PK_StagingDisciplineIncident PRIMARY KEY (DisciplineIncidentKey ASC)   
);
GO

--assessment
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'Assessment' 
			   AND TABLE_SCHEMA = 'Staging')
BEGIN
	CREATE TABLE Staging.Assessment
	(
		AssessmentKey INT NOT NULL IDENTITY(1,1),  
		[_sourceKey] NVARCHAR(2000) NOT NULL, -- EdFi|AssessmentIdentifier|ObjectiveAssessment_IdentificationCode|AssessmentReportingMethodDescriptor_CodeValue
	
		--assessment 
		--------------------------------------------------------------------------------------
		AssessmentCategoryDescriptor_CodeValue NVARCHAR(50) NOT NULL,    
		AssessmentCategoryDescriptor_Description NVARCHAR(1024) NOT NULL,    
		AssessmentFamilyTitle NVARCHAR(100) NULL,    	
		AdaptiveAssessment_Indicator bit NOT NULL, 
		AssessmentIdentifier NVARCHAR(60) NOT NULL,   
		AssessmentTitle NVARCHAR(500) NOT NULL,


		ReportingMethodDescriptor_CodeValue NVARCHAR(50) NOT NULL,   
		ReportingMethodDescriptor_Description NVARCHAR(1024) NOT NULL,   
	
		ResultDatatypeTypeDescriptor_CodeValue  NVARCHAR(50) NOT NULL,   
		ResultDatatypeTypeDescriptor_Description NVARCHAR(1024) NOT NULL,   
	
		AssessmentScore_Indicator  BIT NOT NULL,
		AssessmentPerformanceLevel_Indicator  BIT NOT NULL,

		ObjectiveAssessmentScore_Indicator  BIT NOT NULL,
		ObjectiveAssessmentPerformanceLevel_Indicator  BIT NOT NULL,
	    
		AssessmentModifiedDate [datetime] NOT NULL,

		ValidFrom DATETIME NOT NULL, 
		ValidTo DATETIME NOT NULL, 
		IsCurrent BIT NOT NULL		

		CONSTRAINT PK_StaginAssessment PRIMARY KEY (AssessmentKey)    
	);
	

END;
GO

--course
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'Course' 
			   AND TABLE_SCHEMA = 'Staging')
BEGIN
	CREATE TABLE Staging.Course
	(
		CourseKey INT NOT NULL IDENTITY(1,1),  --surrogate
		[_sourceKey] NVARCHAR(50) NOT NULL,
	
		CourseCode NVARCHAR(60) NOT NULL,
		CourseTitle NVARCHAR(100) NOT NULL,
		CourseDescription NVARCHAR(100) NOT NULL,

		CourseLevelCharacteristicTypeDescriptor_CodeValue NVARCHAR(60) NOT NULL,
		CourseLevelCharacteristicTypeDescriptor_Description NVARCHAR(1024) NOT NULL, 

		AcademicSubjectDescriptor_CodeValue  NVARCHAR(60) NOT NULL,
		AcademicSubjectDescriptor_Description  NVARCHAR(1024) NOT NULL,

		HighSchoolCourseRequirement_Indicator BIT NOT NULL,
		MinimumAvailableCredits INT NULL,
		MaximumAvailableCredits INT NULL,
	
		GPAApplicabilityType_CodeValue NVARCHAR(50) NULL,
		GPAApplicabilityType_Description NVARCHAR(50) NULL,

		SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue NVARCHAR(50) NOT NULL,
		SecondaryCourseLevelCharacteristicTypeDescriptor_Description NVARCHAR(50) NOT NULL,
			
		CourseModifiedDate [datetime] NOT NULL,

		ValidFrom DATETIME NOT NULL, 
		ValidTo DATETIME NOT NULL, 
		IsCurrent BIT NOT NULL		

  		CONSTRAINT PK_StagingCourse PRIMARY KEY (CourseKey)
    
	);	

END;
GO

--attendance by day
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'StudentAttendanceByDay' 
			   AND TABLE_SCHEMA = 'Staging')
CREATE TABLE Staging.StudentAttendanceByDay
(
  StudentAttendanceByDayKey BIGINT IDENTITY(1,1) NOT NULL,
  _sourceKey  NVARCHAR(500) NOT NULL,

  StudentKey INT NULL,
  TimeKey INT NULL,  
  SchoolKey INT NULL,
  AttendanceEventCategoryKey INT NULL,
  AttendanceEventReason nvarchar(500) NULL,
  
  [ModifiedDate] [datetime] NULL,

  _sourceStudentKey NVARCHAR(50) NULL,
  _sourceTimeKey DATE NULL,  
  _sourceSchoolKey NVARCHAR(50) NULL,
  _sourceAttendanceEventCategoryKey NVARCHAR(50) NULL,

  CONSTRAINT PK_StagingStudentAttendanceByDay PRIMARY KEY (StudentAttendanceByDayKey ASC)  
);
GO

--student discipline
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'StudentDiscipline' 
			   AND TABLE_SCHEMA = 'Staging')
CREATE TABLE Staging.StudentDiscipline
(
  StudentDisciplineKey BIGINT IDENTITY(1,1) NOT NULL,
  _sourceKey  NVARCHAR(500) NOT NULL,

  StudentKey INT NULL,
  TimeKey INT NULL, 
  SchoolKey INT NULL,
  DisciplineIncidentKey INT NULL,

  [ModifiedDate] [datetime] NULL,

  _sourceStudentKey NVARCHAR(50) NULL,
  _sourceTimeKey Date NULL, 
  _sourceSchoolKey NVARCHAR(50) NULL,
  _sourceDisciplineIncidentKey NVARCHAR(50) NULL,

  CONSTRAINT PK_StagingStudentDiscipline PRIMARY KEY (StudentDisciplineKey)
);
GO

--student assessment score
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'StudentAssessmentScore' 
			   AND TABLE_SCHEMA = 'Staging')
CREATE TABLE Staging.StudentAssessmentScore
(
  StudentAssessmentScoreKey BIGINT IDENTITY(1,1) NOT NULL,
  _sourceKey  NVARCHAR(500) NOT NULL,

  StudentKey INT NULL,
  TimeKey INT NULL, 
  AssessmentKey INT NULL,

  ScoreResult   NVARCHAR(50) NOT NULL,
  IntegerScoreResult INT NULL,
  DecimalScoreResult FLOAT NULL,
  LiteralScoreResult NVARCHAR(60) NULL,
  
  [ModifiedDate] [datetime] NULL,

  _sourceStudentKey NVARCHAR(50) NULL,
  _sourceTimeKey Date NULL, 
  _sourceAssessmentKey NVARCHAR(500) NULL,

  CONSTRAINT PK_StagingStudentAssessmentScore PRIMARY KEY (StudentAssessmentScoreKey ASC)
  
);
GO

--student course transcript
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'StudentCourseTranscript' 
			   AND TABLE_SCHEMA = 'Staging')
CREATE TABLE Staging.StudentCourseTranscript
(
  StudentCourseTranscriptKey BIGINT IDENTITY(1,1) NOT NULL,
  _sourceKey  NVARCHAR(500) NOT NULL,

  StudentKey INT NULL,
  TimeKey INT NULL, 
  CourseKey INT NULL,
  SchoolKey INT NULL,

  EarnedCredits INT NOT NULL,
  PossibleCredits INT NOT NULL,
  FinalLetterGradeEarned NVARCHAR(10)  NULL,
  FinalNumericGradeEarned DECIMAL(9,2) NULL,
    
  [ModifiedDate] [datetime] NULL,
   
  _sourceStudentKey NVARCHAR(50) NULL,
  _sourceSchoolYear int NULL, 
  _sourceTerm NVARCHAR(50) NULL,
  _sourceCourseKey NVARCHAR(50) NULL,
  _sourceSchoolKey NVARCHAR(50) NULL, 
  
  CONSTRAINT PK_StagingStudentCourseTranscript PRIMARY KEY (StudentCourseTranscriptKey ASC)  
  
);
GO

--Stored Procedures
----------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_Lineage_GetKey]
@LoadType nvarchar(1),
@TableName nvarchar(100)
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

    --current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY

		BEGIN TRANSACTION;   
		
		DECLARE @StartLoad datetime = SYSDATETIME(); -- SYSDATETIME return datetime2 which is more precise
	
		INSERT INTO [dbo].[ETL_Lineage](
			 [TableName]
			,StartTime
			,EndTime
			,[Status]
			,[LoadType]
			)
		VALUES (
			 @TableName
			,@StartLoad
			,NULL
			,'P' --  P = In progress, E = Error, S = Success
			,@LoadType -- F = Full load	- I = Incremental load
			);

		-- If we're doing an initial load, remove the date of the most recent load for this table
		IF (@LoadType = 'F')
			BEGIN
				UPDATE [dbo].[ETL_IncrementalLoads]
				SET LoadDate = '07/01/2015'
				WHERE TableName = @TableName
			END;

		-- Select the key of the previously inserted row
		SELECT MAX([LineageKey]) AS LineageKey
		FROM dbo.[ETL_Lineage]
		WHERE 
			[TableName] = @TableName
			AND StartTime = @StartLoad

	    COMMIT TRANSACTION;		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		IF XACT_STATE( ) = -1
			BEGIN
				--The transaction is in an uncommittable state. Rolling back transaction
				ROLLBACK TRANSACTION;
			END;

		-- Test whether the transaction is committable.
		IF XACT_STATE( ) = 1
			BEGIN
				--The transaction is committable. Committing transaction
				COMMIT TRANSACTION;
			END;
	END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_IncrementalLoads_GetLastLoadedDate]
@TableName nvarchar(100)
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

    --current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY

		BEGIN TRANSACTION;   

			--If the table has not been loaded before, a record is created with the minimum possible date
			IF NOT EXISTS (SELECT 1 
						   FROM [dbo].[ETL_IncrementalLoads] 
						   WHERE TableName = @TableName)
			   BEGIN       
				INSERT INTO [dbo].[ETL_IncrementalLoads](TableName,LoadDate)
				VALUES (@TableName, '07/01/2015')
			   END 

			-- Select the LoadDate for the @TableName
			SELECT 
				[LoadDate] AS [LoadDate]
			FROM [dbo].[ETL_IncrementalLoads]
			WHERE 
				TableName = @TableName;

		COMMIT TRANSACTION;		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		IF XACT_STATE( ) = -1
			BEGIN
				--The transaction is in an uncommittable state. Rolling back transaction
				ROLLBACK TRANSACTION;
			END;

		-- Test whether the transaction is committable.
		IF XACT_STATE( ) = 1
			BEGIN
				--The transaction is committable. Committing transaction
				COMMIT TRANSACTION;
			END;
	END CATCH;
END;
GO

--Dim School
--------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimSchool_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	
		TRUNCATE TABLE Staging.[School]
		INSERT INTO Staging.[School]
				   ([_sourceKey]
				   ,[DistrictSchoolCode]
				   ,[StateSchoolCode]
				   ,[UmbrellaSchoolCode]
				   ,[ShortNameOfInstitution]
				   ,[NameOfInstitution]
				   ,[SchoolCategoryType]
				   ,[SchoolCategoryType_Elementary_Indicator]
				   ,[SchoolCategoryType_Middle_Indicator]
				   ,[SchoolCategoryType_HighSchool_Indicator]
				   ,[SchoolCategoryType_Combined_Indicator]       
				   ,[SchoolCategoryType_Other_Indicator]
				   ,[TitleIPartASchoolDesignationTypeCodeValue]
				   ,[TitleIPartASchoolDesignation_Indicator]
				   ,OperationalStatusTypeDescriptor_CodeValue
				   ,OperationalStatusTypeDescriptor_Description		   

				   ,SchoolNameModifiedDate
 				   ,SchoolOperationalStatusTypeModifiedDate
				   ,SchoolCategoryModifiedDate 
				   ,SchoolTitle1StatusModifiedDate

				   ,[ValidFrom]
				   ,[ValidTo]
				   ,[IsCurrent])
        --declare @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate()
		SELECT  DISTINCT 
			    CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),s.SchoolId)) AS [_sourceKey],
				eoic_sch.IdentificationCode AS DistrictSchoolCode,
				eoic.IdentificationCode AS StateSchoolCode,
				CASE
					WHEN eoic_sch.IdentificationCode IN (1291, 1292, 1293, 1294) THEN '1290'
					when eoic_sch.IdentificationCode IN (1440,1441) THEN '1440' 
					WHEN eoic_sch.IdentificationCode IN (4192,4192) THEN '4192' 
					WHEN eoic_sch.IdentificationCode IN (4031,4033) THEN '4033' 
					WHEN eoic_sch.IdentificationCode IN (1990,1991) THEN '1990' 
					WHEN eoic_sch.IdentificationCode IN (1140,4391) THEN '1140' 
					ELSE eoic_sch.IdentificationCode
				END AS UmbrellaSchoolCode,
				edorg.ShortNameOfInstitution, 
				edorg.NameOfInstitution,
				sct.CodeValue AS SchoolCategoryType, 
				CASE  WHEN sct.CodeValue  IN ('Elementary School') THEN 1 ELSE 0 END  [SchoolCategoryType_Elementary_Indicator],
				CASE  WHEN sct.CodeValue  IN ('Middle School') THEN 1 ELSE 0 END  [SchoolCategoryType_Middle_Indicator],
				CASE  WHEN sct.CodeValue  IN ('High School') THEN 1 ELSE 0 END  [SchoolCategoryType_HighSchool_Indicator],
				CASE  WHEN sct.CodeValue  NOT IN ('Elementary School','Middle School','High School') THEN 1 ELSE 0 END  [SchoolCategoryType_Combined_Indicator],
				0  [SchoolCategoryType_Other_Indicator],
				COALESCE(tIt.CodeValue,'N/A') AS TitleIPartASchoolDesignationTypeCodeValue,
				CASE WHEN tIt.CodeValue NOT IN ('Not designated as a Title I Part A school','N/A') THEN 1 ELSE 0 END AS TitleIPartASchoolDesignation_Indicator,
				COALESCE(ost.CodeValue,'N/A') AS OperationalStatusTypeDescriptor_CodeValue,	
				COALESCE(ost.[Description],'N/A') AS OperationalStatusTypeDescriptor_Description,
				 
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(edorg.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS SchoolNameModifiedDate,
 				CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(ost.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS SchoolOperationalStatusTypeModifiedDate,
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(sct.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS SchoolCategoryModifiedDate,
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(tIt.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS SchoolTitle1StatusModifiedDate,

				--Making sure the first time, the ValidFrom is set to beginning of time 
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN
				           (SELECT MAX(t) FROM
                             (VALUES
                               (edorg.LastModifiedDate)
                             , (ost.LastModifiedDate)
                             , (sct.LastModifiedDate)
                             , (tIt.LastModifiedDate)                             
                             ) AS [MaxLastModifiedDate](t)
                           )
					ELSE 
					      @LastLoadDate -- setting the validFrom to beggining of time during thre first load. 
				END AS ValidFrom,
				'12/31/9999' AS ValidTo,
				CASE WHEN COALESCE(ost.CodeValue,'N/A') IN ('Active','Added','Changed Agency','Continuing','New','Reopened') THEN 1  ELSE 0  END AS IsCurrent		
		--SELECT distinct *
		FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School s
		     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization edorg on s.SchoolId = edorg.EducationOrganizationId
		     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.OperationalStatusType ost ON edorg.OperationalStatusTypeId = ost.OperationalStatusTypeId
		     LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolCategory sc on s.SchoolId = sc.SchoolId
		     LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolCategoryType sct on sc.SchoolCategoryTypeId = sct.SchoolCategoryTypeId
		     LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.TitleIPartASchoolDesignationType tIt on s.TitleIPartASchoolDesignationTypeId = tIt.TitleIPartASchoolDesignationTypeId
		     LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganizationIdentificationCode eoic ON edorg.EducationOrganizationId = eoic.EducationOrganizationId 
		     																			   AND eoic.EducationOrganizationIdentificationSystemDescriptorId = 433 --state code
		     LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganizationIdentificationCode eoic_sch ON edorg.EducationOrganizationId = eoic_sch.EducationOrganizationId 
																					   AND eoic_sch.EducationOrganizationIdentificationSystemDescriptorId = 428 --district code
		WHERE 
			(edorg.LastModifiedDate > @LastLoadDate AND edorg.LastModifiedDate <= @NewLoadDate) OR
			(ost.LastModifiedDate > @LastLoadDate AND ost.LastModifiedDate <= @NewLoadDate) OR
			(sct.LastModifiedDate > @LastLoadDate AND sct.LastModifiedDate <= @NewLoadDate) OR
			(tIt.LastModifiedDate > @LastLoadDate AND tIt.LastModifiedDate <= @NewLoadDate) 	
			
			
			
		--loading legacy data if it has not been loaded.
		--load types are ignored as this data will only be loaded once.
		IF NOT EXISTS(SELECT 1 
		              FROM dbo.DimSchool 
					  WHERE CHARINDEX('LegacyDW',_sourceKey,1) > 0)
			BEGIN
			   INSERT INTO Staging.[School]
				   ([_sourceKey]
				   ,[DistrictSchoolCode]
				   ,[StateSchoolCode]
				   ,[UmbrellaSchoolCode]
				   ,[ShortNameOfInstitution]
				   ,[NameOfInstitution]
				   ,[SchoolCategoryType]
				   ,[SchoolCategoryType_Elementary_Indicator]
				   ,[SchoolCategoryType_Middle_Indicator]
				   ,[SchoolCategoryType_HighSchool_Indicator]
				   ,[SchoolCategoryType_Combined_Indicator]       
				   ,[SchoolCategoryType_Other_Indicator]
				   ,[TitleIPartASchoolDesignationTypeCodeValue]
				   ,[TitleIPartASchoolDesignation_Indicator]
				   ,OperationalStatusTypeDescriptor_CodeValue
				   ,OperationalStatusTypeDescriptor_Description		   

				   ,SchoolNameModifiedDate
 				   ,SchoolOperationalStatusTypeModifiedDate
				   ,SchoolCategoryModifiedDate 
				   ,SchoolTitle1StatusModifiedDate

				   ,[ValidFrom]
				   ,[ValidTo]
				   ,[IsCurrent])
			 SELECT DISTINCT 
				    CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),LTRIM(RTRIM(sd.sch)))) AS [_sourceKey],
					LTRIM(RTRIM(sd.sch)) AS [DistrictSchoolCode],
					CASE WHEN ISNULL(LTRIM(RTRIM(statecd)),'N/A') IN ('','N/A') THEN 'N/A' ELSE ISNULL(LTRIM(RTRIM(statecd)),'N/A') END AS StateSchoolCode,
					CASE
						WHEN LTRIM(RTRIM(sd.sch)) IN ('1291', '1292', '1293', '1294') THEN '1290'
						when LTRIM(RTRIM(sd.sch)) IN ('1440','1441') THEN '1440' 
						WHEN LTRIM(RTRIM(sd.sch)) IN ('4192','4192') THEN '4192' 
						WHEN LTRIM(RTRIM(sd.sch)) IN ('4031','4033') THEN '4033' 
						WHEN LTRIM(RTRIM(sd.sch)) IN ('1990','1991') THEN '1990' 
						WHEN LTRIM(RTRIM(sd.sch)) IN ('1140','4391') THEN '1140' 
						ELSE LTRIM(RTRIM(sd.sch))
					END AS UmbrellaSchoolCode,
					LTRIM(RTRIM(sd.[schname_f]))  AS ShortNameOfInstitution, 
					LTRIM(RTRIM(sd.[schname_f])) AS NameOfInstitution,
					'Combined' AS SchoolCategoryType, 
					0  [SchoolCategoryType_Elementary_Indicator],
					0  [SchoolCategoryType_Middle_Indicator],
					0  [SchoolCategoryType_HighSchool_Indicator],
					1  [SchoolCategoryType_Combined_Indicator],
					0  [SchoolCategoryType_Other_Indicator],
					'N/A' AS TitleIPartASchoolDesignationTypeCodeValue,
					0 AS TitleIPartASchoolDesignation_Indicator,
					'Inactive' AS OperationalStatusTypeDescriptor_CodeValue,	
					'Inactive' AS OperationalStatusTypeDescriptor_Description,

					'07/01/2015' AS SchoolNameModifiedDate,
 				    '07/01/2015' AS SchoolOperationalStatusTypeModifiedDate,
				    '07/01/2015' AS SchoolCategoryModifiedDate,
				    '07/01/2015' AS SchoolTitle1StatusModifiedDate,

					@LastLoadDate AS ValidFrom,
					GETDATE() AS ValidTo,
					0 AS IsCurrent
				--SELECT *
				FROM [Raw_LegacyDW].[SchoolData] sd
				WHERE NOT EXISTS(SELECT 1 
									FROM Staging.[School] ds 
									WHERE 'Ed-Fi|' + Convert(NVARCHAR(MAX),LTRIM(RTRIM(sd.sch))) = ds._sourceKey);
			END

		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

	
	END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimSchool_PopulateProduction]
@LineageKey INT,
@LastDateLoaded DATETIME
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	    
		BEGIN TRANSACTION;   
				 
	     
		--empty row technique
		--fact table should not have null foreign keys references
		--this empty record will be used in those cases
		IF NOT EXISTS (SELECT 1 
		               FROM dbo.DimSchool WHERE _sourceKey = '')
				BEGIN
				   INSERT INTO [dbo].[DimSchool]
							   ([_sourceKey]
							   ,[DistrictSchoolCode]
							   ,[StateSchoolCode]
							   ,[UmbrellaSchoolCode]
							   ,[ShortNameOfInstitution]
							   ,[NameOfInstitution]
							   ,[SchoolCategoryType]
							   ,[SchoolCategoryType_Elementary_Indicator]
							   ,[SchoolCategoryType_Middle_Indicator]
							   ,[SchoolCategoryType_HighSchool_Indicator]
							   ,[SchoolCategoryType_Combined_Indicator]       
							   ,[SchoolCategoryType_Other_Indicator]
							   ,[TitleIPartASchoolDesignationTypeCodeValue]
							   ,[TitleIPartASchoolDesignation_Indicator]
							   ,OperationalStatusTypeDescriptor_CodeValue
							   ,OperationalStatusTypeDescriptor_Description		   
							   ,[ValidFrom]
							   ,[ValidTo]
							   ,[IsCurrent]
							   ,[LineageKey])
				    VALUES
					(   N'',       -- _sourceKey - nvarchar(50)
						N'N/A',       -- DistrictSchoolCode - nvarchar(10)
						N'N/A',       -- StateSchoolCode - nvarchar(50)
						N'N/A',       -- UmbrellaSchoolCode - nvarchar(50)
						N'N/A',       -- ShortNameOfInstitution - nvarchar(500)
						N'N/A',       -- NameOfInstitution - nvarchar(500)
						N'N/A',       -- SchoolCategoryType - nvarchar(100)
						0,      -- SchoolCategoryType_Elementary_Indicator - bit
						0,      -- SchoolCategoryType_Middle_Indicator - bit
						0,      -- SchoolCategoryType_HighSchool_Indicator - bit
						0,      -- SchoolCategoryType_Combined_Indicator - bit
						0,      -- SchoolCategoryType_Other_Indicator - bit
						N'N/A', -- TitleIPartASchoolDesignationTypeCodeValue - nvarchar(50)
						0,      -- TitleIPartASchoolDesignation_Indicator - bit
						N'N/A',       -- OperationalStatusTypeDescriptor_CodeValue - nvarchar(50)
						N'N/A',       -- OperationalStatusTypeDescriptor_Description - nvarchar(1024)
						'07/01/2015', -- ValidFrom - datetime
						'9999-12-31', -- ValidTo - datetime
						0,      -- IsCurrent - bit
						-1          -- LineageKey - int
						)
				    
				END

		
		--staging table holds newer records. 
		--the matching prod records will be valid until the date in which the newest data change was identified		
		UPDATE prod
		SET prod.ValidTo = stage.ValidFrom
		FROM 
			[dbo].[DimSchool] AS prod
			INNER JOIN Staging.School AS stage ON prod._sourceKey = stage._sourceKey
		WHERE prod.ValidTo = '12/31/9999'


		INSERT INTO dbo.DimSchool
		(
		    _sourceKey,
		    DistrictSchoolCode,
		    StateSchoolCode,
		    UmbrellaSchoolCode,
		    ShortNameOfInstitution,
		    NameOfInstitution,
		    SchoolCategoryType,
		    SchoolCategoryType_Elementary_Indicator,
		    SchoolCategoryType_Middle_Indicator,
		    SchoolCategoryType_HighSchool_Indicator,
		    SchoolCategoryType_Combined_Indicator,
		    SchoolCategoryType_Other_Indicator,
		    TitleIPartASchoolDesignationTypeCodeValue,
		    TitleIPartASchoolDesignation_Indicator,
		    OperationalStatusTypeDescriptor_CodeValue,
		    OperationalStatusTypeDescriptor_Description,
		    ValidFrom,
		    ValidTo,
		    IsCurrent,
		    LineageKey
		)
		SELECT 
		    _sourceKey,
		    DistrictSchoolCode,
		    StateSchoolCode,
		    UmbrellaSchoolCode,
		    ShortNameOfInstitution,
		    NameOfInstitution,
		    SchoolCategoryType,
		    SchoolCategoryType_Elementary_Indicator,
		    SchoolCategoryType_Middle_Indicator,
		    SchoolCategoryType_HighSchool_Indicator,
		    SchoolCategoryType_Combined_Indicator,
		    SchoolCategoryType_Other_Indicator,
		    TitleIPartASchoolDesignationTypeCodeValue,
		    TitleIPartASchoolDesignation_Indicator,
		    OperationalStatusTypeDescriptor_CodeValue,
		    OperationalStatusTypeDescriptor_Description,
		    ValidFrom,
		    ValidTo,
		    IsCurrent,
		    @LineageKey
		FROM Staging.School

		-- updating the EndTime to now and status to Success		
		UPDATE dbo.ETL_Lineage
			SET 
				EndTime = SYSDATETIME(),
				Status = 'S' -- success
		WHERE [LineageKey] = @LineageKey;
	
	
		-- Update the LoadDates table with the most current load date
		UPDATE [dbo].[ETL_IncrementalLoads]
		SET [LoadDate] = @LastDateLoaded
		WHERE [TableName] = N'dbo.DimSchool';

		COMMIT TRANSACTION;		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		IF XACT_STATE( ) = -1
			BEGIN
				--The transaction is in an uncommittable state. Rolling back transaction
				ROLLBACK TRANSACTION;
			END;

		-- Test whether the transaction is committable.
		IF XACT_STATE( ) = 1
			BEGIN
				--The transaction is committable. Committing transaction
				COMMIT TRANSACTION;
			END;
	END CATCH;
END;
GO

-- Dim Time
-------------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimTime_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY

		  

		TRUNCATE TABLE Staging.[Time]

		IF CONVERT(date, @LastLoadDate) < CONVERT(date, getdate())
		 BEGIN

			--non-school
			DECLARE @NonSchoolTime TABLE
			(
			  SchoolDate DATE NOT NULL , -- 9/1/2019
			  SchoolDate_MMYYYY CHAR(6) NOT NULL,
			  SchoolDate_Fomat1 CHAR(10) NOT NULL,
			  SchoolDate_Fomat2 CHAR(8) NOT NULL,
			  SchoolDate_Fomat3 CHAR(10) NOT NULL,
    
			  SchoolYear SMALLINT NOT NULL, -- ex: 9/1/2019 = 2020  
			  SchoolYearDescription NVARCHAR(50) NOT NULL, -- '2019-2020 or SchoolYear 2019 - 2020'
			  CalendarYear SMALLINT NOT NULL, -- ex: 9/1/2019 = 2020
  
			  DayOfMonth TINYINT NOT NULL, -- 1 - 30|31
			  DaySuffix CHAR(2) NOT NULL , -- 1st, 2nd, 3rd
			  DayName  NVARCHAR(15) NOT NULL, -- Monday, Tuesday    
			  DayNameShort NVARCHAR(15) NOT NULL, -- Mon, Tue
			  DayOfWeek TINYINT NOT NULL, -- 1 - 7
  
			  WeekInMonth TINYINT NOT NULL, -- 1 - 4 or 5 -- this counts 7 days starting on the 1st of the month
			  WeekOfMonth TINYINT NOT NULL, -- 1 - 4 or 5 -- this is the actual week of the month (starting on sunday)
			  Weekend_Indicator BIT NOT NULL,
			  WeekOfYear TINYINT NOT NULL, -- 1 - 53   
			  FirstDayOfWeek DATE NOT NULL,
			  LastDayOfWeek DATE NOT NULL,
			  WeekBeforeChristmas_Indicator BIT NOT NULL, --  True,False
  
  
			  [Month] TINYINT NOT NULL, -- 1..12
			  MonthName  NVARCHAR(10) NOT NULL, --January,February,December
			  MonthNameShort CHAR(3) NOT NULL, --Jan,Feb,Dec  
			  FirstDayOfMonth DATE NOT NULL,
			  LastDayOfMonth DATE NOT NULL,
			  FirstDayOfNextMonth DATE NOT NULL,
			  LastDayOfNextMonth DATE NOT NULL,
    
			  DayOfYear SMALLINT NOT NULL, -- 1 - 365 or 366 (Leap Year Every Four Years)  
			  LeapYear_Indicator BIT NOT NULL,  
    
			  FederalHolidayName NVARCHAR(20) NULL, -- Memorial Day
			  FederalHoliday_Indicator BIT NOT NULL --  True,False  
			);




			DECLARE @startDate DATE = @LastLoadDate;
			DECLARE @endDate DATE = @NewLoadDate;

			--generating time fields that don't vary by school;
			WITH daySeqs (daySeqNumber)
			AS (SELECT 0
				UNION ALL
				SELECT daySeqNumber + 1
				FROM daySeqs
				WHERE daySeqNumber < DATEDIFF(DAY, @startDate, @endDate)),
				 theDates (theDate)
			AS (SELECT DATEADD(DAY, daySeqNumber, @startDate)
				FROM daySeqs),
				 src
			AS (SELECT TheDate = CONVERT(DATE, theDate),
					   TheDay = DATEPART(DAY, theDate),
					   TheDayName = DATENAME(WEEKDAY, theDate),
					   TheWeek = DATEPART(WEEK, theDate),
					   TheDayOfWeek = DATEPART(WEEKDAY, theDate),
					   TheMonth = DATEPART(MONTH, theDate),
					   TheMonthName = DATENAME(MONTH, theDate),
					   TheQuarter = DATEPART(QUARTER, theDate),
					   TheYear = DATEPART(YEAR, theDate),
					   TheFirstOfMonth = DATEFROMPARTS(YEAR(theDate), MONTH(theDate), 1),
					   TheLastOfYear = DATEFROMPARTS(YEAR(theDate), 12, 31),
					   TheDayOfYear = DATEPART(DAYOFYEAR, theDate)
				FROM theDates),
				 timeDim
			AS (SELECT [SchoolDate] = TheDate,
					   SchoolDate_MMYYYY = CONVERT(CHAR(2), CONVERT(CHAR(8), TheDate, 101)) + CONVERT(CHAR(4), TheYear),
					   SchoolDate_Fomat1 = CONVERT(CHAR(10), TheDate, 101),
					   SchoolDate_Fomat2 = CONVERT(CHAR(8), TheDate, 112),
					   SchoolDate_Fomat3 = CONVERT(CHAR(10), TheDate, 120),
					   SchoolYear = dbo.Func_ETL_GetSchoolYear(TheDate),
					   SchoolYearDescription = CONVERT(NVARCHAR(MAX), dbo.Func_ETL_GetSchoolYear(TheDate) - 1) + ' - '
											   + CONVERT(NVARCHAR(MAX), dbo.Func_ETL_GetSchoolYear(TheDate)),
					   CalendarYear = TheYear,
					   [DayOfMonth] = TheDay,
					   DaySuffix = CONVERT(   CHAR(2),
											  CASE
												  WHEN TheDay / 10 = 1 THEN
													  'th'
												  ELSE
													  CASE RIGHT(TheDay, 1)
														  WHEN '1' THEN
															  'st'
														  WHEN '2' THEN
															  'nd'
														  WHEN '3' THEN
															  'rd'
														  ELSE
															  'th'
													  END
											  END
										  ),
					   [DayName] = TheDayName,
					   DayNameShort = FORMAT(TheDate, 'ddd'),
					   [DayOfWeek] = TheDayOfWeek,
					   WeekInMonth = CONVERT(
												TINYINT,
												ROW_NUMBER() OVER (PARTITION BY TheFirstOfMonth, TheDayOfWeek ORDER BY TheDate)
											),
					   WeekOfMonth = CONVERT(TINYINT, DENSE_RANK() OVER (PARTITION BY TheYear, TheMonth ORDER BY TheWeek)),
					   Weekend_Indicator = CASE
											   WHEN TheDayOfWeek IN (   CASE @@DATEFIRST
																			WHEN 1 THEN
																				6
																			WHEN 7 THEN
																				1
																		END, 7
																	) THEN
												   1
											   ELSE
												   0
										   END,
					   WeekOfYear = TheWeek,
					   FirstDayOfWeek = DATEADD(DAY, 1 - TheDayOfWeek, TheDate),
					   LastDayOfWeek = DATEADD(DAY, 6, DATEADD(DAY, 1 - TheDayOfWeek, TheDate)),
					   WeekBeforeChristmas_Indicator = CASE
														   WHEN DATEPART(WEEK, TheDate) = DATEPART(WEEK, '12-25-' + CONVERT(NVARCHAR(4),TheYear)) - 1 THEN
															   1
														   ELSE
															   0
													   END,
					   [Month] = TheMonth,
					   [MonthName] = TheMonthName,
					   MonthNameShort = FORMAT(TheDate, 'MMM'),
					   FirstDayOfMonth = TheFirstOfMonth,
					   LastDayOfMonth = MAX(TheDate) OVER (PARTITION BY TheYear, TheMonth),
					   FirstDayOfNextMonth = DATEADD(MONTH, 1, TheFirstOfMonth),
					   LastDayOfNextMonth = DATEADD(DAY, -1, DATEADD(MONTH, 2, TheFirstOfMonth)),
					   [DayOfYear] = TheDayOfYear,
					   DayOfSchoolYear = TheDayOfYear, --change this 
					   LeapYear_Indicator = CONVERT(   BIT,
													   CASE
														   WHEN (TheYear % 400 = 0)
																OR
																(
																	TheYear % 4 = 0
																	AND TheYear % 100 <> 0
																) THEN
															   1
														   ELSE
															   0
													   END
												   ),
					   FederalHolidayName = [dbo].[Func_ETL_GetHolidayFromDate](TheDate), -- Memorial Day, 4th of July
					   FederalHoliday_Indicator = (CASE
														WHEN [dbo].[Func_ETL_GetHolidayFromDate](TheDate) = 'Non-Holiday' THEN
															0
														ELSE
															1
													END
												   )                                   --  True,False			  


				FROM src)

			INSERT INTO @NonSchoolTime ([SchoolDate]
									   ,[SchoolDate_MMYYYY]
									   ,[SchoolDate_Fomat1]
									   ,[SchoolDate_Fomat2]
									   ,[SchoolDate_Fomat3]
									   ,[SchoolYear]
									   ,[SchoolYearDescription]
									   ,[CalendarYear]
									   ,[DayOfMonth]
									   ,[DaySuffix]
									   ,[DayName]
									   ,[DayNameShort]
									   ,[DayOfWeek]
									   ,[WeekInMonth]
									   ,[WeekOfMonth]
									   ,[Weekend_Indicator]
									   ,[WeekOfYear]
									   ,[FirstDayOfWeek]
									   ,[LastDayOfWeek]
									   ,[WeekBeforeChristmas_Indicator]
									   ,[Month]
									   ,[MonthName]
									   ,[MonthNameShort]
									   ,[FirstDayOfMonth]
									   ,[LastDayOfMonth]
									   ,[FirstDayOfNextMonth]
									   ,[LastDayOfNextMonth]
									   ,[DayOfYear]
									   ,[LeapYear_Indicator]
									   ,[FederalHolidayName]
									   ,[FederalHoliday_Indicator])
			SELECT [SchoolDate]
				  ,[SchoolDate_MMYYYY]
				  ,[SchoolDate_Fomat1]
				  ,[SchoolDate_Fomat2]
				  ,[SchoolDate_Fomat3]
				  ,[SchoolYear]
				  ,[SchoolYearDescription]
				  ,[CalendarYear]
				  ,[DayOfMonth]
				  ,[DaySuffix]
				  ,[DayName]
				  ,[DayNameShort]
				  ,[DayOfWeek]
				  ,[WeekInMonth]
				  ,[WeekOfMonth]
				  ,[Weekend_Indicator]
				  ,[WeekOfYear]
				  ,[FirstDayOfWeek]
				  ,[LastDayOfWeek]
				  ,[WeekBeforeChristmas_Indicator]
				  ,[Month]
				  ,[MonthName]
				  ,[MonthNameShort]
				  ,[FirstDayOfMonth]
				  ,[LastDayOfMonth]
				  ,[FirstDayOfNextMonth]
				  ,[LastDayOfNextMonth]
				  ,[DayOfYear]
				  ,[LeapYear_Indicator]
				  ,[FederalHolidayName]
				  ,[FederalHoliday_Indicator]
			FROM timeDim
			ORDER BY [SchoolDate]
			OPTION (MAXRECURSION 0);

			--;WITH EdFiSchools AS
			--( 
				--DECLARE @startDate DATE = '20150701';    
				SELECT DISTINCT 
						cd.Date as SchoolDate, 	
					   CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),s.SchoolId)) AS [_sourceKey],
					   --ses.SessionName,
					   td.CodeValue TermDescriptorCodeValue, 
					   td.Description TermDescriptorDescription,       
					   cet.CodeValue CalendarEventTypeCodeValue,
					   cet.Description CalendarEventTypeDescription, 
					   ses.LastModifiedDate AS SchoolSessisonModifiedDate, -- school sessions changes are ignored for BPS
					   cet.LastModifiedDate AS CalendarEventTypeModifiedDate,
					   DENSE_RANK() OVER (PARTITION BY ses.SchoolYear, s.SchoolId ORDER BY cd.Date) AS DayOfSchoolYear  INTO #EdFiSchools
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School s
					INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization edOrg  ON s.SchoolId = edOrg.EducationOrganizationId
					INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CalendarDate cd ON s.SchoolId = cd.SchoolId
					INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CalendarDateCalendarEvent cdce ON cd.SchoolId = cdce.SchoolId
																										AND cd.Date = cdce.Date
					INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CalendarEventDescriptor ced  ON cdce.CalendarEventDescriptorId = ced.CalendarEventDescriptorId
					INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor cedv  ON ced.CalendarEventDescriptorId = cedv.DescriptorId
					INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CalendarEventType cet ON ced.CalendarEventTypeId = cet.CalendarEventTypeId
					INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Session ses ON s.SchoolId = ses.SchoolId
																					 AND cd.Date BETWEEN ses.BeginDate AND ses.EndDate
					INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor td ON ses.TermDescriptorId = td.DescriptorId

				WHERE  cd.Date >= @startDate AND 
					  (
					   (ses.LastModifiedDate > @LastLoadDate AND ses.LastModifiedDate <= @NewLoadDate) OR 
					   (cet.LastModifiedDate > @LastLoadDate AND cet.LastModifiedDate <= @NewLoadDate)
					  )
			--)
		
			INSERT INTO Staging.[Time]
					   ([SchoolDate]
					   ,[SchoolDate_MMYYYY]
					   ,[SchoolDate_Fomat1]
					   ,[SchoolDate_Fomat2]
					   ,[SchoolDate_Fomat3]
					   ,[SchoolYear]
					   ,[SchoolYearDescription]
					   ,[CalendarYear]
					   ,[DayOfMonth]
					   ,[DaySuffix]
					   ,[DayName]
					   ,[DayNameShort]
					   ,[DayOfWeek]
					   ,[WeekInMonth]
					   ,[WeekOfMonth]
					   ,[Weekend_Indicator]
					   ,[WeekOfYear]
					   ,[FirstDayOfWeek]
					   ,[LastDayOfWeek]
					   ,[WeekBeforeChristmas_Indicator]
					   ,[Month]
					   ,[MonthName]
					   ,[MonthNameShort]
					   ,[FirstDayOfMonth]
					   ,[LastDayOfMonth]
					   ,[FirstDayOfNextMonth]
					   ,[LastDayOfNextMonth]
					   ,[DayOfYear]
					   ,[LeapYear_Indicator]
					   ,[FederalHolidayName]
					   ,[FederalHoliday_Indicator]
                   
					   ,SchoolSourceKey
					   ,DayOfSchoolYear
					   ,SchoolCalendarEventType_CodeValue
					   ,SchoolCalendarEventType_Description
					   ,SchoolTermDescriptor_CodeValue
					   ,SchoolTermDescriptor_Description
		           
					   ,SchoolSessisonModifiedDate
					   ,CalendarEventTypeModifiedDate

					   ,[ValidFrom]
					   ,[ValidTo]
					   ,[IsCurrent])
			select nst.[SchoolDate]
				  ,nst.[SchoolDate_MMYYYY]
				  ,nst.[SchoolDate_Fomat1]
				  ,nst.[SchoolDate_Fomat2]
				  ,nst.[SchoolDate_Fomat3]
				  ,nst.[SchoolYear]
				  ,nst.[SchoolYearDescription]
				  ,nst.[CalendarYear]
				  ,nst.[DayOfMonth]
				  ,nst.[DaySuffix]
				  ,nst.[DayName]
				  ,nst.[DayNameShort]
				  ,nst.[DayOfWeek]
				  ,nst.[WeekInMonth]
				  ,nst.[WeekOfMonth]
				  ,nst.[Weekend_Indicator]
				  ,nst.[WeekOfYear]
				  ,nst.[FirstDayOfWeek]
				  ,nst.[LastDayOfWeek]
				  ,nst.[WeekBeforeChristmas_Indicator]
				  ,nst.[Month]
				  ,nst.[MonthName]
				  ,nst.[MonthNameShort]
				  ,nst.[FirstDayOfMonth]
				  ,nst.[LastDayOfMonth]
				  ,nst.[FirstDayOfNextMonth]
				  ,nst.[LastDayOfNextMonth]
				  ,nst.[DayOfYear]
				  ,nst.[LeapYear_Indicator]
				  ,nst.[FederalHolidayName]
				  ,nst.[FederalHoliday_Indicator]

				  ,es.[_sourceKey] AS SchoolSourceKey
				  ,es.DayOfSchoolYear
				  ,es.CalendarEventTypeCodeValue
				  ,es.CalendarEventTypeDescription
				  ,es.TermDescriptorCodeValue
				  ,es.TermDescriptorDescription	  

				  ,COALESCE(es.SchoolSessisonModifiedDate,'07/01/2015') AS SchoolSessisonModifiedDate -- school sessions are ignore for BPS
				  ,COALESCE(es.CalendarEventTypeModifiedDate,'07/01/2015')  AS CalendarEventTypeModifiedDate
 
				  ,CASE WHEN @LastLoadDate <> '07/01/2015' THEN
							  COALESCE(
							  (SELECT MAX(t) FROM
								 (VALUES
								   (es.SchoolSessisonModifiedDate)
								 , (es.CalendarEventTypeModifiedDate)                             
								 ) AS [MaxLastModifiedDate](t)
							   ),'07/01/2015')
						ELSE 
							  '07/01/2015' -- setting the validFrom to beginning of time during thre first load. 
					END AS ValidFrom
				   ,'12/31/9999'   AS ValidTo
				   , 1 AS IsCurrent
			FROM @NonSchoolTime nst
				 LEFT JOIN #EdFiSchools es ON nst.SchoolDate = es.SchoolDate			 
			DROP table #EdFiSchools
		 END

		


			
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		
	END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimTime_PopulateProduction]
@LineageKey INT,
@LastDateLoaded DATETIME
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	    
		BEGIN TRANSACTION;   
		
		
		--staging table holds newer records. 
		--the matching prod records will be valid until the date in which the newest data change was identified
		UPDATE prod
		SET prod.ValidTo = stage.ValidFrom
		FROM 
			[dbo].[DimTime] AS prod
			INNER JOIN Staging.[Time] AS stage ON prod.SchoolDate = stage.SchoolDate
		WHERE prod.ValidTo = '12/31/9999'

		INSERT INTO dbo.DimTime
		(
		    [SchoolDate]
           ,[SchoolDate_MMYYYY]
           ,[SchoolDate_Fomat1]
           ,[SchoolDate_Fomat2]
           ,[SchoolDate_Fomat3]
           ,[SchoolYear]
           ,[SchoolYearDescription]
           ,[CalendarYear]
           ,[DayOfMonth]
           ,[DaySuffix]
           ,[DayName]
           ,[DayNameShort]
           ,[DayOfWeek]
           ,[WeekInMonth]
           ,[WeekOfMonth]
           ,[Weekend_Indicator]
           ,[WeekOfYear]
           ,[FirstDayOfWeek]
           ,[LastDayOfWeek]
           ,[WeekBeforeChristmas_Indicator]
           ,[Month]
           ,[MonthName]
           ,[MonthNameShort]
           ,[FirstDayOfMonth]
           ,[LastDayOfMonth]
           ,[FirstDayOfNextMonth]
           ,[LastDayOfNextMonth]
           ,[DayOfYear]
           ,[LeapYear_Indicator]
           ,[FederalHolidayName]
           ,[FederalHoliday_Indicator]
           
		   ,[SchoolKey]
		   ,DayOfSchoolYear
           ,SchoolCalendarEventType_CodeValue
           ,SchoolCalendarEventType_Description
           ,SchoolTermDescriptor_CodeValue
           ,SchoolTermDescriptor_Description
		   
           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey]
		)
		SELECT 
		    st.[SchoolDate]
           ,st.[SchoolDate_MMYYYY]
           ,st.[SchoolDate_Fomat1]
           ,st.[SchoolDate_Fomat2]
           ,st.[SchoolDate_Fomat3]
           ,st.[SchoolYear]
           ,st.[SchoolYearDescription]
           ,st.[CalendarYear]
           ,st.[DayOfMonth]
           ,st.[DaySuffix]
           ,st.[DayName]
           ,st.[DayNameShort]
           ,st.[DayOfWeek]
           ,st.[WeekInMonth]
           ,st.[WeekOfMonth]
           ,st.[Weekend_Indicator]
           ,st.[WeekOfYear]
           ,st.[FirstDayOfWeek]
           ,st.[LastDayOfWeek]
           ,st.[WeekBeforeChristmas_Indicator]
           ,st.[Month]
           ,st.[MonthName]
           ,st.[MonthNameShort]
           ,st.[FirstDayOfMonth]
           ,st.[LastDayOfMonth]
           ,st.[FirstDayOfNextMonth]
           ,st.[LastDayOfNextMonth]
           ,st.[DayOfYear]
           ,st.[LeapYear_Indicator]
           ,st.[FederalHolidayName]
           ,st.[FederalHoliday_Indicator]
           ,ds.SchoolKey		   
		   ,st.DayOfSchoolYear
           ,st.SchoolCalendarEventType_CodeValue
           ,st.SchoolCalendarEventType_Description
           ,st.SchoolTermDescriptor_CodeValue
           ,st.SchoolTermDescriptor_Description
		   
           ,st.[ValidFrom]
           ,st.[ValidTo]
           ,st.[IsCurrent]
		   ,@LineageKey
		FROM Staging.[Time] st
		     LEFT JOIN dbo.DimSchool ds ON st.SchoolSourceKey = ds._sourceKey

		-- updating the EndTime to now and status to Success		
		UPDATE dbo.ETL_Lineage
			SET 
				EndTime = SYSDATETIME(),
				Status = 'S' -- success
		WHERE [LineageKey] = @LineageKey;
	
	
		-- Update the LoadDates table with the most current load date
		UPDATE [dbo].[ETL_IncrementalLoads]
		SET [LoadDate] = @LastDateLoaded
		WHERE [TableName] = N'dbo.DimTime';

		COMMIT TRANSACTION;		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		IF XACT_STATE( ) = -1
			BEGIN
				--The transaction is in an uncommittable state. Rolling back transaction
				ROLLBACK TRANSACTION;
			END;

		-- Test whether the transaction is committable.
		IF XACT_STATE( ) = 1
			BEGIN
				--The transaction is committable. Committing transaction
				COMMIT TRANSACTION;
			END;
	END CATCH;
END;
GO

-- Dim Staff
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimStaff_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	    --declare @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate()
		TRUNCATE TABLE Staging.Staff
		INSERT INTO Staging.Staff
		(
		    _sourceKey,
		    PrimaryElectronicMailAddress,
		    PrimaryElectronicMailTypeDescriptor_CodeValue,
		    PrimaryElectronicMailTypeDescriptor_Description,
		    StaffUniqueId,
		    PersonalTitlePrefix,
		    FirstName,
		    MiddleName,
		    MiddleInitial,
		    LastSurname,
		    FullName,
		    GenerationCodeSuffix,
		    MaidenName,
		    BirthDate,
		    StaffAge,
		    SexType_Code,
		    SexType_Description,
		    SexType_Male_Indicator,
		    SexType_Female_Indicator,
		    SexType_NotSelected_Indicator,
		    HighestLevelOfEducationDescriptorDescriptor_CodeValue,
		    HighestLevelOfEducationDescriptorDescriptor_Description,
		    YearsOfPriorProfessionalExperience,
		    YearsOfPriorTeachingExperience,
		    HighlyQualifiedTeacher_Indicator,
		    StaffClassificationDescriptor_CodeValue,
		    StaffClassificationDescriptor_CodeDescription,			
			StaffMainInfoModifiedDate,
		    ValidFrom,
		    ValidTo,
		    IsCurrent
		)
	
        
		SELECT  DISTINCT 
			    CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),s.StaffUSI)) AS [_sourceKey]
				,sem.ElectronicMailAddress AS [PrimaryElectronicMailAddress]
				,emt.CodeValue AS [PrimaryElectronicMailTypeDescriptor_CodeValue]
				,emt.Description AS [PrimaryElectronicMailTypeDescriptor_Description]
				,s.StaffUniqueId
				,s.PersonalTitlePrefix
				,s.FirstName
				,s.MiddleName
				,LEFT(LTRIM(s.MiddleName),1) AS MiddleInitial	    
				,s.LastSurname
				,dbo.Func_ETL_GetFullName(s.FirstName,s.MiddleName,s.LastSurname) AS FullName
				,s.GenerationCodeSuffix
				,s.MaidenName        
				,s.BirthDate
				,DATEDIFF(YEAR, s.BirthDate, GetDate()) AS [StaffAge]
				,CASE 
					WHEN sex.CodeValue  = 'Male' THEN 'M'
					WHEN sex.CodeValue  = 'Female' THEN 'F'
					ELSE 'NS' -- not selected
				END AS SexType_Code
				,COALESCE(sex.CodeValue,'Not Selected') AS SexType_Description
				,CASE WHEN COALESCE(sex.CodeValue,'Not Selected')  = 'Male' THEN 1 ELSE 0 END AS SexType_Male_Indicator
				,CASE WHEN COALESCE(sex.CodeValue,'Not Selected')  = 'Female' THEN 1 ELSE 0 END AS SexType_Female_Indicator
				,CASE WHEN COALESCE(sex.CodeValue,'Not Selected')  = 'Not Selected' THEN 1 ELSE 0 END AS SexType_NotSelected_Indicator
				,COALESCE(d_le.CodeValue,'N/A') as [HighestLevelOfEducationDescriptorDescriptor_CodeValue]
				,COALESCE(d_le.Description,'N/A') as [HighestLevelOfEducationDescriptorDescriptor_Description]
				,s.YearsOfPriorProfessionalExperience
				,s.YearsOfPriorTeachingExperience
				,s.HighlyQualifiedTeacher
				,COALESCE(d_sc.CodeValue,'N/A') as StaffClassificationDescriptor_CodeValue
				,COALESCE(d_sc.Description,'N/A') as StaffClassificationDescriptor_Description
				,CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(s.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS StaffMainInfoModifiedDate
				--Making sure the first time, the ValidFrom is set to beginning of time 
				,CASE WHEN @LastLoadDate <> '07/01/2015' THEN
				           (SELECT MAX(t) FROM
                             (VALUES
                               (s.LastModifiedDate)
                             ) AS [MaxLastModifiedDate](t)
                           )
					ELSE 
					      seoaa.BeginDate 
				END AS ValidFrom
				,case when seoaa.EndDate IS null then  '12/31/9999' else seoaa.EndDate  END AS ValidTo
				,case when seoaa.EndDate IS NULL OR seoaa.EndDate > GETDATE() THEN  1 else 0 end AS IsCurrent 
		--SELECT distinct *
		FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Staff s 
			  INNER JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffEducationOrganizationAssignmentAssociation seoaa ON s.StaffUSI = seoaa.StaffUSI
			  --sex	 
			  left JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SexType sex ON s.SexTypeId = sex.SexTypeId
			  left join [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.OldEthnicityType oet on s.OldEthnicityTypeId = oet.OldEthnicityTypeId
			  left join [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CitizenshipStatusType cst on s.CitizenshipStatusTypeId = cst.CitizenshipStatusTypeId
			  left join [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d_le on s.HighestCompletedLevelOfEducationDescriptorId = d_le.DescriptorId
			  LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffElectronicMail sem ON s.StaffUSI = sem.StaffUSI
			 															  AND sem.PrimaryEmailAddressIndicator = 1
			  LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ElectronicMailType emt ON sem.ElectronicMailTypeId = emt.ElectronicMailTypeId
			  left join [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d_sc on seoaa.StaffClassificationDescriptorId = d_sc.DescriptorId
			 
		WHERE 
			(s.LastModifiedDate > @LastLoadDate AND s.LastModifiedDate <= @NewLoadDate)
						
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

	
	END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimStaff_PopulateProduction]
@LineageKey INT,
@LastDateLoaded DATETIME
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	    
		BEGIN TRANSACTION;   
				 
	     
		--empty row technique
		--fact table should not have null foreign keys references
		--this empty record will be used in those cases
		IF NOT EXISTS (SELECT 1 
		               FROM dbo.DimStaff WHERE _sourceKey = '')
				BEGIN
				   INSERT INTO [dbo].DimStaff
				   (
				       _sourceKey,
				       PrimaryElectronicMailAddress,
				       PrimaryElectronicMailTypeDescriptor_CodeValue,
				       PrimaryElectronicMailTypeDescriptor_Description,
				       StaffUniqueId,
				       PersonalTitlePrefix,
				       FirstName,
				       MiddleName,
				       MiddleInitial,
				       LastSurname,
				       FullName,
				       GenerationCodeSuffix,
				       MaidenName,
				       BirthDate,
				       StaffAge,
				       SexType_Code,
				       SexType_Description,
				       SexType_Male_Indicator,
				       SexType_Female_Indicator,
				       SexType_NotSelected_Indicator,
				       HighestLevelOfEducationDescriptorDescriptor_CodeValue,
				       HighestLevelOfEducationDescriptorDescriptor_Description,
				       YearsOfPriorProfessionalExperience,
				       YearsOfPriorTeachingExperience,
				       HighlyQualifiedTeacher_Indicator,
				       StaffClassificationDescriptor_CodeValue,
				       StaffClassificationDescriptor_CodeDescription,
				       ValidFrom,
				       ValidTo,
				       IsCurrent,
				       LineageKey
				   )
				   VALUES
				   (   N'',       -- _sourceKey - nvarchar(50)
				       N'N/A',       -- PrimaryElectronicMailAddress - nvarchar(128)
				       N'N/A',       -- PrimaryElectronicMailTypeDescriptor_CodeValue - nvarchar(128)
				       N'N/A',       -- PrimaryElectronicMailTypeDescriptor_Description - nvarchar(128)
				       N'N/A',       -- StaffUniqueId - nvarchar(32)
				       N'N/A',       -- PersonalTitlePrefix - nvarchar(30)
				       N'N/A',       -- FirstName - nvarchar(75)
				       N'N/A',       -- MiddleName - nvarchar(75)
				       '',        -- MiddleInitial - char(1)
				       N'N/A',       -- LastSurname - nvarchar(75)
				       N'N/A',       -- FullName - nvarchar(50)
				       N'N/A',       -- GenerationCodeSuffix - nvarchar(10)
				       N'N/A',       -- MaidenName - nvarchar(75)
				       GETDATE(), -- BirthDate - date
				       0,         -- StaffAge - int
				       N'',       -- SexType_Code - nvarchar(15)
				       N'',       -- SexType_Description - nvarchar(100)
				       0,      -- SexType_Male_Indicator - bit
				       0,      -- SexType_Female_Indicator - bit
				       1,      -- SexType_NotSelected_Indicator - bit
				       N'',       -- HighestLevelOfEducationDescriptorDescriptor_CodeValue - nvarchar(100)
				       N'',       -- HighestLevelOfEducationDescriptorDescriptor_Description - nvarchar(100)
				       NULL,      -- YearsOfPriorProfessionalExperience - decimal(5, 2)
				       NULL,      -- YearsOfPriorTeachingExperience - decimal(5, 2)
				       NULL,      -- HighlyQualifiedTeacher_Indicator - bit
				       N'',       -- StaffClassificationDescriptor_CodeValue - nvarchar(100)
				       N'',       -- StaffClassificationDescriptor_CodeDescription - nvarchar(100)
				      '07/01/2015', -- ValidFrom - datetime
					  '9999-12-31', -- ValidTo - datetime
					   0,      -- IsCurrent - bit
					  -1          -- LineageKey - int
				       )
				END

		
		--staging table holds newer records. 
		--the matching prod records will be valid until the date in which the newest data change was identified		
		UPDATE prod
		SET prod.ValidTo = stage.ValidFrom
		FROM 
			[dbo].[DimSchool] AS prod
			INNER JOIN Staging.School AS stage ON prod._sourceKey = stage._sourceKey
		WHERE prod.ValidTo = '12/31/9999'


		INSERT INTO dbo.DimStaff
		(
		    _sourceKey,
		    PrimaryElectronicMailAddress,
		    PrimaryElectronicMailTypeDescriptor_CodeValue,
		    PrimaryElectronicMailTypeDescriptor_Description,
		    StaffUniqueId,
		    PersonalTitlePrefix,
		    FirstName,
		    MiddleName,
		    MiddleInitial,
		    LastSurname,
		    FullName,
		    GenerationCodeSuffix,
		    MaidenName,
		    BirthDate,
		    StaffAge,
		    SexType_Code,
		    SexType_Description,
		    SexType_Male_Indicator,
		    SexType_Female_Indicator,
		    SexType_NotSelected_Indicator,
		    HighestLevelOfEducationDescriptorDescriptor_CodeValue,
		    HighestLevelOfEducationDescriptorDescriptor_Description,
		    YearsOfPriorProfessionalExperience,
		    YearsOfPriorTeachingExperience,
		    HighlyQualifiedTeacher_Indicator,
		    StaffClassificationDescriptor_CodeValue,
		    StaffClassificationDescriptor_CodeDescription,
		    ValidFrom,
		    ValidTo,
		    IsCurrent,
		    LineageKey
		)
		
		SELECT 
		    _sourceKey,
		    PrimaryElectronicMailAddress,
		    PrimaryElectronicMailTypeDescriptor_CodeValue,
		    PrimaryElectronicMailTypeDescriptor_Description,
		    StaffUniqueId,
		    PersonalTitlePrefix,
		    FirstName,
		    MiddleName,
		    MiddleInitial,
		    LastSurname,
		    FullName,
		    GenerationCodeSuffix,
		    MaidenName,
		    BirthDate,
		    StaffAge,
		    SexType_Code,
		    SexType_Description,
		    SexType_Male_Indicator,
		    SexType_Female_Indicator,
		    SexType_NotSelected_Indicator,
		    HighestLevelOfEducationDescriptorDescriptor_CodeValue,
		    HighestLevelOfEducationDescriptorDescriptor_Description,
		    YearsOfPriorProfessionalExperience,
		    YearsOfPriorTeachingExperience,
		    HighlyQualifiedTeacher_Indicator,
		    StaffClassificationDescriptor_CodeValue,
		    StaffClassificationDescriptor_CodeDescription,
		    ValidFrom,
		    ValidTo,
		    IsCurrent,		    
		    @LineageKey
		FROM Staging.Staff

		-- updating the EndTime to now and status to Success		
		UPDATE dbo.ETL_Lineage
			SET 
				EndTime = SYSDATETIME(),
				Status = 'S' -- success
		WHERE [LineageKey] = @LineageKey;
	
	
		-- Update the LoadDates table with the most current load date
		UPDATE [dbo].[ETL_IncrementalLoads]
		SET [LoadDate] = @LastDateLoaded
		WHERE [TableName] = N'dbo.DimStaff';

		COMMIT TRANSACTION;		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		IF XACT_STATE( ) = -1
			BEGIN
				--The transaction is in an uncommittable state. Rolling back transaction
				ROLLBACK TRANSACTION;
			END;

		-- Test whether the transaction is committable.
		IF XACT_STATE( ) = 1
			BEGIN
				--The transaction is committable. Committing transaction
				COMMIT TRANSACTION;
			END;
	END CATCH;
END;
GO

-- Staff Access
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_StaffAccess_PopulateProduction]
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	    
		
		DECLARE @currenSchoolYear INT
		SELECT TOP(1) @currenSchoolYear = syt.SchoolYear
		FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolYearType syt
		WHERE syt.CurrentSchoolYear = 1 
		ORDER BY syt.SchoolYear DESC

		CREATE TABLE #currentYearStaff_DistrictAdmins(StaffSourceKey NVARCHAR(50))
		INSERT INTO #currentYearStaff_DistrictAdmins(StaffSourceKey)
		SELECT DISTINCT CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),ssa.StaffUSI))
		FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffSchoolAssociation ssa
		WHERE ssa.SchoolYear = @currenSchoolYear
		  AND ssa.SchoolId = 9035 -- central office BPS;

		CREATE TABLE #currentYearStaff_Teachers(StaffSourceKey NVARCHAR(50))
		INSERT INTO #currentYearStaff_Teachers(StaffSourceKey)
		SELECT DISTINCT CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),ssa.StaffUSI))
		FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffSchoolAssociation ssa
		WHERE ssa.SchoolYear = @currenSchoolYear
		  AND EXISTS (SELECT 1
		                  FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffSectionAssociation s_sect_a
						  WHERE ssa.StaffUSI = s_sect_a.StaffUSI
						    AND ssa.SchoolYear = s_sect_a.SchoolYear)
          AND NOT EXISTS (SELECT 1
						  FROM  #currentYearStaff_DistrictAdmins da
						  WHERE  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),ssa.StaffUSI)) = da.StaffSourceKey);

		CREATE TABLE #currentYearStaff_SchoolAdmins(StaffSourceKey NVARCHAR(50), SchoolSourceKey NVARCHAR(50))
		INSERT INTO #currentYearStaff_SchoolAdmins(StaffSourceKey, SchoolSourceKey)
		SELECT DISTINCT CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),ssa.StaffUSI)) ,
		                 CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),ssa.SchoolId)) 
		FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffSchoolAssociation ssa
		WHERE ssa.SchoolYear = @currenSchoolYear
		  AND ssa.SchoolId <> 9035 -- central office BPS;
		  AND NOT EXISTS (SELECT 1
						  FROM  #currentYearStaff_DistrictAdmins da
						  WHERE  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),ssa.StaffUSI)) = da.StaffSourceKey)
		  AND NOT EXISTS (SELECT 1
						  FROM  #currentYearStaff_Teachers t
						  WHERE  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),ssa.StaffUSI)) = t.StaffSourceKey);


		



		--Staff Current Schools
		-------------------------------------------------------------------------------------
		--dropping the columnstore index
		DROP INDEX IF EXISTS CSI_Derived_StaffCurrentSchool ON [Derived].[StaffCurrentSchools];
		
		TRUNCATE TABLE [Derived].[StaffCurrentSchools];
		
		--district admins - they can see all schools except "Central Office BPS"
		INSERT INTO [Derived].[StaffCurrentSchools]
		(
		    [StaffKey],
		    [SchoolKey]
		)		
		SELECT DISTINCT 
		       ds.StaffKey,
		       dschool.SchoolKey
		FROM dbo.DimStaff ds
		     CROSS JOIN dbo.DimSchool dschool
		WHERE 1=1 --ssa.StaffUSI = 9786
		  AND ds.IsCurrent = 1		  
		  AND dschool.DistrictSchoolCode <>  '9035' -- central office BPS
		  AND dschool.IsCurrent = 1
		  AND EXISTS (SELECT 1 
		              FROM #currentYearStaff_DistrictAdmins t
					  WHERE ds._sourceKey = t.StaffSourceKey)


		--non-district admins - they can see only their respective schools
		INSERT INTO [Derived].[StaffCurrentSchools]
		(
		    [StaffKey],
		    [SchoolKey]
		)
		SELECT DISTINCT 
		       ds.StaffKey,
		       dschool.SchoolKey
		FROM dbo.DimStaff ds
		     INNER join #currentYearStaff_SchoolAdmins sa ON ds._sourceKey	= sa.StaffSourceKey
			 INNER JOIN dbo.DimSchool dschool ON sa.SchoolSourceKey  = dschool._sourceKey	 
		WHERE 1=1 --ssa.StaffUSI = 9786
		  AND ds.IsCurrent = 1		  
		  AND dschool.IsCurrent = 1
        
	    --re-creating the columnstore index
		CREATE COLUMNSTORE INDEX CSI_Derived_StaffCurrentSchool  ON [Derived].[StaffCurrentSchools] ( [StaffKey],[SchoolKey])		
		
        

		--Staff Current GradeLevels
		----------------------------------------------------------------------------------------
		--dropping the columnstore index
		DROP INDEX IF EXISTS CSI_Derived_StaffCurrentGradeLevels ON [Derived].StaffCurrentGradeLevels;

		TRUNCATE TABLE Derived.StaffCurrentGradeLevels

		--district admins - they can see all schools except "Central Office BPS"
		;WITH allGradeLevels AS
        (
		  SELECT DISTINCT case GradeLevelDescriptor_CodeValue
		  			when 'Eighth grade' then 	'08'
		  			when 'Eleventh grade' then 	'11'
		  			when 'Fifth grade' then 	'05'
		  			when 'First grade' then 	'01'
		  			when 'Fourth grade' then 	'04'
		  			when 'Kindergarten'  then 'K'
		  			when 'Ninth grade' then 	'09'
		  			when 'Preschool/Prekindergarten' then 'PK'
		  			when 'Second grade' then 	'02'
		  			when 'Seventh grade' then 	'07'
		  			when 'Sixth grade' then 	'06'
		  			when 'Tenth grade' then 	'10'
		  			when 'Third grade' then 	'03'
		  			when 'Twelfth grade' then 	'12'
					when '' then 	'Unknown'
		  			ELSE GradeLevelDescriptor_CodeValue 
				  end  AS GradeLevel
		  FROM dbo.DimStudent 
		  WHERE IsCurrent = 1 
		) 
		INSERT INTO Derived.StaffCurrentGradeLevels
	    (
	        StaffKey,
	        GradeLevel
	    )
        
		SELECT DISTINCT 
		       ds.StaffKey,
		       scgl.GradeLevel
		FROM dbo.DimStaff ds		     
			 CROSS JOIN allGradeLevels scgl 
		WHERE 1=1 --ssa.StaffUSI = 9786
		  AND ds.IsCurrent = 1
		  AND EXISTS (SELECT 1 
		              FROM #currentYearStaff_DistrictAdmins t
					  WHERE ds._sourceKey = t.StaffSourceKey)
		  

		--school admins  they can only see the grade levels of their respective schools
	    INSERT INTO Derived.StaffCurrentGradeLevels
	    (
	        StaffKey,
	        GradeLevel
	    )
		SELECT DISTINCT 
		       ds.StaffKey,
			   case dst.GradeLevelDescriptor_CodeValue
		  			when 'Eighth grade' then 	'08'
		  			when 'Eleventh grade' then 	'11'
		  			when 'Fifth grade' then 	'05'
		  			when 'First grade' then 	'01'
		  			when 'Fourth grade' then 	'04'
		  			when 'Kindergarten'  then 'K'
		  			when 'Ninth grade' then 	'09'
		  			when 'Preschool/Prekindergarten' then 'PK'
		  			when 'Second grade' then 	'02'
		  			when 'Seventh grade' then 	'07'
		  			when 'Sixth grade' then 	'06'
		  			when 'Tenth grade' then 	'10'
		  			when 'Third grade' then 	'03'
		  			when 'Twelfth grade' then 	'12'
					when '' then 	'Unknown'
		  			ELSE dst.GradeLevelDescriptor_CodeValue 
				  end  AS GradeLevel
		      
		FROM dbo.DimStaff ds
		     INNER join #currentYearStaff_SchoolAdmins sa ON ds._sourceKey	= sa.StaffSourceKey
			 INNER JOIN dbo.DimSchool dschool ON  sa.SchoolSourceKey  = dschool._sourceKey	
			 INNER JOIN dbo.DimStudent dst ON dschool.SchoolKey	 = dst.SchoolKey
		WHERE 1=1 --ssa.StaffUSI = 9786
		  AND ds.IsCurrent = 1		  
		  AND dst.IsCurrent = 1
		  AND dschool.IsCurrent = 1;
		

	   --teachers
		INSERT INTO Derived.StaffCurrentGradeLevels
	    (
	        StaffKey,
	        GradeLevel
	    )
		SELECT DISTINCT 
		       ds.StaffKey,
			    case dst.GradeLevelDescriptor_CodeValue
		  			when 'Eighth grade' then 	'08'
		  			when 'Eleventh grade' then 	'11'
		  			when 'Fifth grade' then 	'05'
		  			when 'First grade' then 	'01'
		  			when 'Fourth grade' then 	'04'
		  			when 'Kindergarten'  then 'K'
		  			when 'Ninth grade' then 	'09'
		  			when 'Preschool/Prekindergarten' then 'PK'
		  			when 'Second grade' then 	'02'
		  			when 'Seventh grade' then 	'07'
		  			when 'Sixth grade' then 	'06'
		  			when 'Tenth grade' then 	'10'
		  			when 'Third grade' then 	'03'
		  			when 'Twelfth grade' then 	'12'
					when '' then 	'Unknown'
		  			ELSE dst.GradeLevelDescriptor_CodeValue 
				  end  AS GradeLevel	      
		FROM dbo.DimStaff ds
		     INNER join [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffSectionAssociation  sa_staff ON ds._sourceKey	= CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),sa_staff.StaffUSI))  
			 INNER join [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSectionAssociation sa_stud ON sa_staff.UniqueSectionCode = sa_stud.UniqueSectionCode	
			                                                                                       AND  sa_staff.SchoolId = sa_stud.SchoolId
																								   AND  sa_staff.SchoolYear = sa_stud.SchoolYear
			 INNER JOIN dbo.DimStudent dst ON CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),sa_stud.StudentUSI)) = dst._sourceKey

		WHERE 1=1 --ssa.StaffUSI = 9786
		  AND ds.IsCurrent = 1		  
		  AND dst.IsCurrent = 1		  
		  AND EXISTS (SELECT 1 
		              FROM #currentYearStaff_Teachers t
					  WHERE ds._sourceKey = t.StaffSourceKey)
		  AND sa_staff.SchoolYear = @currenSchoolYear
		 


        CREATE COLUMNSTORE INDEX CSI_Derived_StaffCurrentGradeLevels  ON [Derived].StaffCurrentGradeLevels ( [StaffKey],GradeLevel)		

		--Staff Current Students
		---------------------------------------------------------------------------------------
		--dropping the columnstore index
		DROP INDEX IF EXISTS CSI_Derived_StaffCurrentStudents ON [Derived].StaffCurrentStudents;

		TRUNCATE TABLE Derived.StaffCurrentStudents

		--district admins
		INSERT INTO Derived.StaffCurrentStudents
		(
		    StaffKey,
		    StudentKey
		)
		SELECT DISTINCT 
		       ds.StaffKey,
			   dst.StudentKey		      
		FROM dbo.DimStaff ds		     
			 CROSS JOIN dbo.DimStudent dst 
		WHERE 1=1 --ssa.StaffUSI = 9786
		  AND ds.IsCurrent = 1
		  AND dst.IsCurrent = 1
		  AND EXISTS (SELECT 1 
		              FROM #currentYearStaff_DistrictAdmins t
					  WHERE ds._sourceKey = t.StaffSourceKey)
		


		--school admins
		INSERT INTO Derived.StaffCurrentStudents
		(
		    StaffKey,
		    StudentKey
		)
		SELECT DISTINCT 
		       ds.StaffKey,
			   dst.StudentKey		      
		FROM dbo.DimStaff ds
		     INNER join #currentYearStaff_SchoolAdmins sa ON ds._sourceKey	= sa.StaffSourceKey 
			 INNER JOIN dbo.DimSchool dschool ON  sa.SchoolSourceKey  = dschool._sourceKey	
			 INNER JOIN dbo.DimStudent dst ON dschool.SchoolKey	 = dst.SchoolKey
		WHERE 1=1 --ssa.StaffUSI = 9786
		  AND ds.IsCurrent = 1
		  AND dst.IsCurrent = 1		  
		  

		--teachers
		INSERT INTO Derived.StaffCurrentStudents
		(
		    StaffKey,
		    StudentKey
		)
		SELECT DISTINCT 
		       ds.StaffKey,
			   dst.StudentKey		      
		FROM dbo.DimStaff ds
		     INNER join [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffSectionAssociation  sa_staff ON ds._sourceKey	= CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),sa_staff.StaffUSI))  
			 INNER join [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSectionAssociation sa_stud ON sa_staff.UniqueSectionCode = sa_stud.UniqueSectionCode	
			                                                                                       AND  sa_staff.SchoolId = sa_stud.SchoolId
																								   AND  sa_staff.SchoolYear = sa_stud.SchoolYear
			 INNER JOIN dbo.DimStudent dst ON CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),sa_stud.StudentUSI)) = dst._sourceKey

		WHERE 1=1 --ssa.StaffUSI = 9786
		  AND ds.IsCurrent = 1		  
		  AND dst.IsCurrent = 1		  
		  AND EXISTS (SELECT 1 
		              FROM #currentYearStaff_Teachers t
					  WHERE ds._sourceKey = t.StaffSourceKey)
		  AND sa_staff.SchoolYear = @currenSchoolYear
		
					 
        CREATE COLUMNSTORE INDEX CSI_Derived_StaffCurrentStudents  ON [Derived].StaffCurrentStudents ( [StaffKey],StudentKey)		

	    DROP TABLE IF EXISTS #currentYearStaff_DistrictAdmins;
		DROP TABLE IF EXISTS #currentYearStaff_SchoolAdmins;
		DROP TABLE IF EXISTS #currentYearStaff_Teachers;
	
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
	END CATCH;
END;
GO

--Dim Student
--------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimStudent_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY

		--BEGIN TRANSACTION;   

		TRUNCATE TABLE Staging.[Student]

		--DECLARE @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate();
		SELECT DISTINCT s.StudentUSI INTO #StudentsWithChanges
		FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s		      
		      INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa ON s.StudentUSI = ssa.StudentUSI
        WHERE  ssa.SchoolYear >= 2019 AND
			   (
				(s.LastModifiedDate > @LastLoadDate AND s.LastModifiedDate <= @NewLoadDate) OR
				(ssa.LastModifiedDate > @LastLoadDate AND ssa.LastModifiedDate <= @NewLoadDate)			 
			   )

		--DECLARE @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate();
		SELECT DISTINCT 
			   s.StudentUSI, 
			   COUNT(sr.StudentUSI) AS RaceCount,
			   STRING_AGG(rt.CodeValue,',') AS RaceCodes,
			   STRING_AGG(rt.Description,',') AS RaceDescriptions,
			   CASE WHEN EXISTS (SELECT 1 
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr
									   WHERE s.StudentUSI = sr.StudentUSI
										 AND sr.RaceTypeId = 1) THEN 1
			   ELSE 
				   0	             
			   END AS Race_AmericanIndianAlaskanNative_Indicator,
			   CASE WHEN EXISTS (SELECT 1 
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr
									   WHERE s.StudentUSI = sr.StudentUSI
										 AND sr.RaceTypeId = 2) THEN 1
			   ELSE 
				   0	             
			   END AS Race_Asian_Indicator,
			   CASE WHEN EXISTS (SELECT 1 
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr
									   WHERE s.StudentUSI = sr.StudentUSI
										 AND sr.RaceTypeId = 3) THEN 1
			   ELSE 
				   0	             
			   END AS Race_BlackAfricaAmerican_Indicator,
			   CASE WHEN EXISTS (SELECT 1 
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr
									   WHERE s.StudentUSI = sr.StudentUSI
										 AND sr.RaceTypeId = 5) THEN 1
			   ELSE 
				   0	             
			   END AS Race_NativeHawaiianPacificIslander_Indicator,
			   CASE WHEN EXISTS (SELECT 1 
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr
									   WHERE s.StudentUSI = sr.StudentUSI
										 AND sr.RaceTypeId = 7) THEN 1
			   ELSE 
				   0	             
			   END AS Race_White_Indicator,
			   CASE WHEN EXISTS (SELECT 1 
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr
									   WHERE s.StudentUSI = sr.StudentUSI
										 AND sr.RaceTypeId = 4) THEN 1
			   ELSE 
				   0	             
			   END AS Race_ChooseNotRespond_Indicator,
			   CASE WHEN EXISTS (SELECT 1 
								 FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr
									   WHERE s.StudentUSI = sr.StudentUSI
										 AND sr.RaceTypeId = 6) THEN 1
			   ELSE 
				   0	             
			   END AS Race_Other_Indicator into #StudentRaces    

		FROM  #StudentsWithChanges s     	
			  LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentRace sr ON s.StudentUSI = sr.StudentUSI		
			  LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.RaceType rt ON sr.RaceTypeId = rt.RaceTypeId	   
		GROUP BY s.StudentUSI
				
				
		--;WITH StudentHomeRooomByYear AS
		--(
			SELECT DISTINCT std_sa.StudentUSI, 
							std_sa.SchoolYear, 
							std_sa.SchoolId,  
							std_sa.ClassroomIdentificationCode AS HomeRoom,
							dbo.Func_ETL_GetFullName(staff.FirstName,staff.MiddleName,staff.LastSurname) AS HomeRoomTeacher,
							ROW_NUMBER() OVER (PARTITION BY std_sa.StudentUSI, 
															std_sa.SchoolYear, 
															std_sa.SchoolId ORDER BY staff_sa.BeginDate DESC) AS RowRankId INTO #StudentHomeRooomByYear
			FROM #StudentsWithChanges s
			     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSectionAssociation std_sa ON s.StudentUSI = std_sa.StudentUSI			
				 INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffSectionAssociation staff_sa  ON std_sa.UniqueSectionCode = staff_sa.UniqueSectionCode
																										AND std_sa.SchoolYear = staff_sa.SchoolYear
				 INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Staff staff on staff_sa.StaffUSI = staff.StaffUSI
			WHERE std_sa.HomeroomIndicator = 1
				 AND std_sa.SchoolYear >= 2019
				 AND std_sa.EndDate > GETDATE()
				 --AND s.StudentUniqueId = 269159 
				
					 
        --)
		
		INSERT INTO Staging.[Student]
				   ([_sourceKey]
				   ,[PrimaryElectronicMailAddress]
				   ,[PrimaryElectronicMailTypeDescriptor_CodeValue]
				   ,[PrimaryElectronicMailTypeDescriptor_Description]
				   ,[StudentUniqueId]
				   ,[StateId]
				   ,[SchoolKey]
				   ,[ShortNameOfInstitution]
				   ,[NameOfInstitution]
				   ,[GradeLevelDescriptor_CodeValue]
				   ,[GradeLevelDescriptor_Description]
				   ,[FirstName]
				   ,[MiddleInitial]
				   ,[MiddleName]
				   ,[LastSurname]
				   ,[FullName]
				   ,[BirthDate]
				   ,[StudentAge]
				   ,[GraduationSchoolYear]
				   ,[Homeroom]
				   ,[HomeroomTeacher]
				   ,[SexType_Code]
				   ,[SexType_Description]
				   ,[SexType_Male_Indicator]
				   ,[SexType_Female_Indicator]
				   ,[SexType_NotSelected_Indicator]
				   ,[RaceCode]
				   ,[RaceDescription]
				   ,[StateRaceCode]
				   ,[Race_AmericanIndianAlaskanNative_Indicator]
				   ,[Race_Asian_Indicator]
				   ,[Race_BlackAfricaAmerican_Indicator]
				   ,[Race_NativeHawaiianPacificIslander_Indicator]
				   ,[Race_White_Indicator]
				   ,[Race_MultiRace_Indicator]
				   ,[Race_ChooseNotRespond_Indicator]
				   ,[Race_Other_Indicator]
				   ,[EthnicityCode]
				   ,[EthnicityDescription]
				   ,[EthnicityHispanicLatino_Indicator]
				   ,[Migrant_Indicator]
				   ,[Homeless_Indicator]
				   ,[IEP_Indicator]
				   ,[English_Learner_Code_Value]
				   ,[English_Learner_Description]
				   ,[English_Learner_Indicator]
				   ,[Former_English_Learner_Indicator]
				   ,[Never_English_Learner_Indicator]
				   ,[EconomicDisadvantage_Indicator]
				   ,[EntryDate]
				   ,[EntrySchoolYear]
				   ,[EntryCode]
				   ,[ExitWithdrawDate]
				   ,[ExitWithdrawSchoolYear]
				   ,[ExitWithdrawCode]	   

				   
				   ,StudentMainInfoModifiedDate
	               ,StudentSchoolAssociationModifiedDate

				   ,[ValidFrom]
				   ,[ValidTo]
				   ,[IsCurrent])
        --DECLARE @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate();
		SELECT distinct
			   CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),s.StudentUSI)) AS [_sourceKey],
			   sem.ElectronicMailAddress AS [PrimaryElectronicMailAddress],
			   emt.CodeValue AS [PrimaryElectronicMailTypeDescriptor_CodeValue],
			   emt.Description AS [PrimaryElectronicMailTypeDescriptor_Description],
			   s.StudentUniqueId,       
			   sic.IdentificationCode AS StateId,
       
			   dschool.SchoolKey,
			   edorg.ShortNameOfInstitution,
			   edorg.NameOfInstitution,
			   gld.CodeValue GradeLevelDescriptor_CodeValue,
			   gld.Description AS GradeLevelDescriptor_Description,

			   s.FirstName,
			   LEFT(LTRIM(s.MiddleName),1) AS MiddleInitial,
			   s.MiddleName,	   
			   s.LastSurname,
			   dbo.Func_ETL_GetFullName(s.FirstName,s.MiddleName,s.LastSurname) AS FullName,
			   s.BirthDate,
			   DATEDIFF(YEAR, s.BirthDate, GetDate()) AS StudentAge,
			   ssa.GraduationSchoolYear,

			   COALESCE(shrby.Homeroom,'N/A') AS Homeroom,
			   COALESCE(shrby.HomeroomTeacher,'N/A') AS HomerHomeroomTeacheroom,
			   
			   CASE 
					WHEN sex.CodeValue  = 'Male' THEN 'M'
					WHEN sex.CodeValue  = 'Female' THEN 'F'
					ELSE 'NS' -- not selected
			   END AS SexType_Code,
			   sex.Description AS SexType_Description,
			   CASE WHEN sex.CodeValue  = 'Male' THEN 1 ELSE 0 END AS SexType_Male_Indicator,
			   CASE WHEN sex.CodeValue  = 'Female' THEN 1 ELSE 0 END AS SexType_Female_Indicator,
			   CASE WHEN sex.CodeValue  = 'Not Selected' THEN 1 ELSE 0 END AS SexType_NotSelected_Indicator, 
       
			   COALESCE(sr.RaceCodes,'N/A') AS RaceCode,	   
			   COALESCE(sr.RaceDescriptions,'N/A') AS RaceDescription,
			   CASE WHEN sr.RaceCount > 1 AND s.HispanicLatinoEthnicity = 0 THEN 'Multirace' 
					WHEN s.HispanicLatinoEthnicity = 1 THEN 'Latinx'
					ELSE COALESCE(sr.RaceCodes,'N/A')
			   END AS StateRaceCode,
			   sr.Race_AmericanIndianAlaskanNative_Indicator,
			   sr.Race_Asian_Indicator ,

			   sr.Race_BlackAfricaAmerican_Indicator,
			   sr.Race_NativeHawaiianPacificIslander_Indicator,
			   sr.Race_White_Indicator,
			   CASE WHEN sr.RaceCount > 1 AND s.HispanicLatinoEthnicity = 0 THEN 1 ELSE 0 END AS Race_MultiRace_Indicator, 
			   sr.Race_ChooseNotRespond_Indicator,
			   sr.Race_Other_Indicator,

			   CASE WHEN s.HispanicLatinoEthnicity = 1 THEN 'L' ELSE 'Non-L' END  AS EthnicityCode,
			   CASE WHEN s.HispanicLatinoEthnicity = 1 THEN 'Latinx' ELSE 'Non Latinx' END  AS EthnicityDescription,
			   s.HispanicLatinoEthnicity AS EthnicityHispanicLatino_Indicator,

			   CASE WHEN EXISTS (
							   SELECT 1
							   FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentProgramAssociation spa
							   WHERE CHARINDEX('Migrant', spa.ProgramName,1) > 1
									 AND spa.StudentUSI = s.StudentUSI
									 AND (
									         spa.EndDate IS NULL
									      OR spa.EndDate > CAST(GETDATE() AS DATE)
									     )
						   ) THEN 1 ELSE 0 End AS Migrant_Indicator,
			   CASE WHEN EXISTS (
							   SELECT 1
							   FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentProgramAssociation spa
							   WHERE CHARINDEX('Homeless', spa.ProgramName,1) > 1
									 AND spa.StudentUSI = s.StudentUSI
									  AND (
									         spa.EndDate IS NULL
									      OR spa.EndDate > CAST(GETDATE() AS DATE)
									     )
									 AND spa.EndDate IS NULL
						   ) THEN 1 ELSE 0 End AS Homeless_Indicator,
				CASE WHEN EXISTS (
							   SELECT 1
							   FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSpecialEducationProgramAssociation spa
							   WHERE CHARINDEX('504 Plan', spa.ProgramName,1) = 0
									 AND spa.StudentUSI = s.StudentUSI
									 AND (
									         spa.IEPEndDate IS NULL
									      OR spa.IEPEndDate > CAST(GETDATE() AS DATE)
									     )
						   ) THEN 1 ELSE 0 End AS IEP_Indicator,
	   
			   COALESCE(lepd.CodeValue,'N/A') AS LimitedEnglishProficiencyDescriptor_CodeValue,
			   COALESCE(lepd.CodeValue,'N/A') AS LimitedEnglishProficiencyDescriptor_Description,
			   CASE WHEN COALESCE(lepd.CodeValue,'N/A') = 'Limited' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_EnglishLearner_Indicator,
			   CASE WHEN COALESCE(lepd.CodeValue,'N/A') = 'Formerly Limited' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_Former_Indicator,
			   CASE WHEN COALESCE(lepd.CodeValue,'N/A') = 'NotLimited' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_NotEnglisLearner_Indicator,

			   COALESCE(s.EconomicDisadvantaged,0) AS EconomicDisadvantage_Indicator,
	   
			   --entry
			   ssa.EntryDate,
			   dbo.Func_ETL_GetSchoolYear((ssa.EntryDate)) AS EntrySchoolYear, 
			   COALESCE(eglrt.CodeValue,'N/A') AS EntryCode,
       
			   --exit
			   ssa.ExitWithdrawDate,
			   dbo.Func_ETL_GetSchoolYear((ssa.ExitWithdrawDate)) AS ExitWithdrawSchoolYear, 
			   ewt.CodeValue ExitWithdrawCode,              

			   CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(s.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS SchoolCategoryModifiedDate,
			   CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(ssa.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS SchoolTitle1StatusModifiedDate,

				
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN
				           (SELECT MAX(t) FROM
                             (VALUES
                               (s.LastModifiedDate)
                             , (ssa.LastModifiedDate)                                                    
                             ) AS [MaxLastModifiedDate](t)
                           )
					ELSE 
					      ssa.EntryDate
				END AS ValidFrom,
			   CASE when ssa.ExitWithdrawDate is null then '12/31/9999'  else ssa.ExitWithdrawDate END  AS ValidTo,
			   case when ssa.ExitWithdrawDate is NULL AND EXISTS(SELECT 1 FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolYearType syt WHERE syt.CurrentSchoolYear = 1 AND syt.SchoolYear = ssa.SchoolYear) then 1 else 0 end AS IsCurrent
			   	
		--select ssa.SchoolYear, ssa.ExitWithdrawDate , case  when ssa.ExitWithdrawDate is NULL and EXISTS(SELECT 1 FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolYearType syt WHERE syt.CurrentSchoolYear = 1 AND syt.SchoolYear = ssa.SchoolYear)   then 1 else 0 end 
		FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s
			INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa ON s.StudentUSI = ssa.StudentUSI
			INNER JOIN dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ssa.SchoolId)   = dschool._sourceKey
			INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor gld  ON ssa.EntryGradeLevelDescriptorId = gld.DescriptorId			
			INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization edorg ON ssa.SchoolId = edorg.EducationOrganizationId

			LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EntryGradeLevelReasonType eglrt ON ssa.EntryGradeLevelReasonTypeId = eglrt.EntryGradeLevelReasonTypeId
			LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ExitWithdrawTypeDescriptor ewtd ON ssa.ExitWithdrawTypeDescriptorId = ewtd.ExitWithdrawTypeDescriptorId
			LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor ewtdd ON ewtd.ExitWithdrawTypeDescriptorId = ewtdd.DescriptorId
			LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ExitWithdrawType ewt ON ewtd.ExitWithdrawTypeId = ewt.ExitWithdrawTypeId
			LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentElectronicMail sem ON s.StudentUSI = sem.StudentUSI
																		   AND sem.PrimaryEmailAddressIndicator = 1
			LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ElectronicMailType emt ON sem.ElectronicMailTypeId = emt.ElectronicMailTypeId
			

			--lunch
			LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor food ON s.SchoolFoodServicesEligibilityDescriptorId = food.DescriptorId
			--sex
			INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SexType sex ON s.SexTypeId = sex.SexTypeId
			--state id
			LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentIdentificationCode sic ON s.StudentUSI = sic.StudentUSI
																							   AND sic.AssigningOrganizationIdentificationCode = 'State' 
			--lep
			LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor lepd ON s.LimitedEnglishProficiencyDescriptorId = lepd.DescriptorId
	
			--races
			LEFT JOIN #StudentRaces sr ON s.StudentUSI = sr.StudentUSI
			
			--homeroom
			LEFT JOIN #StudentHomeRooomByYear shrby ON  s.StudentUSI = shrby.StudentUSI
												   AND ssa.SchoolId = shrby.SchoolId
												   AND ssa.SchoolYear = shrby.SchoolYear
												   AND shrby.RowRankId = 1
	
		WHERE ssa.SchoolYear >= 2019 AND
		     (
			   (s.LastModifiedDate > @LastLoadDate AND s.LastModifiedDate <= @NewLoadDate) OR
			   (ssa.LastModifiedDate > @LastLoadDate AND ssa.LastModifiedDate <= @NewLoadDate)			 
			 )
			 
		DROP TABLE #StudentRaces, #StudentHomeRooomByYear, #StudentsWithChanges;
				
			
			
		--loading legacy data if it has not been loaded.
		--load types are ignored as this data will only be loaded once.
		IF NOT EXISTS(SELECT 1 
		              FROM dbo.DimStudent 
					  WHERE CHARINDEX('LegacyDW',_sourceKey,1) > 0)
			BEGIN
			    ;WITH HomelessStudentsByYear AS (
				--Sch year 2015:
				  SELECT studentno, 2016 AS schyear
				  FROM [BPSGranary02].[RMUStudentBackup].[dbo].[Homeless2015Final] 
				  WHERE McKinneyVento = 'Y'
				  UNION ALL 
				--Sch year 2016:
				  SELECT studentno, 2017 AS schyear
				  FROM [BPSGranary02].[RMUStudentBackup].[dbo].[Homeless2016Final] 
				  WHERE McKinneyVento = 'Y'  
				  UNION ALL
				--Sch year 2017:
				  SELECT studentno, 2018 AS schyear
				  FROM [BPSGranary02].[RMUStudentBackup].[dbo].[Homeless2017Final] 
				  WHERE McKinneyVento = 'Y'  
				)
			   INSERT INTO Staging.[Student]
				   ([_sourceKey]
				   ,[PrimaryElectronicMailAddress]
				   ,[PrimaryElectronicMailTypeDescriptor_CodeValue]
				   ,[PrimaryElectronicMailTypeDescriptor_Description]
				   ,[StudentUniqueId]
				   ,[StateId]
				   ,[SchoolKey]
				   ,[ShortNameOfInstitution]
				   ,[NameOfInstitution]
				   ,[GradeLevelDescriptor_CodeValue]
				   ,[GradeLevelDescriptor_Description]
				   ,[FirstName]
				   ,[MiddleInitial]
				   ,[MiddleName]
				   ,[LastSurname]
				   ,[FullName]
				   ,[BirthDate]
				   ,[StudentAge]
				   ,[GraduationSchoolYear]
				   ,[Homeroom]
				   ,[HomeroomTeacher]
				   ,[SexType_Code]
				   ,[SexType_Description]
				   ,[SexType_Male_Indicator]
				   ,[SexType_Female_Indicator]
				   ,[SexType_NotSelected_Indicator]
				   ,[RaceCode]
				   ,[RaceDescription]
				   ,[StateRaceCode]
				   ,[Race_AmericanIndianAlaskanNative_Indicator]
				   ,[Race_Asian_Indicator]
				   ,[Race_BlackAfricaAmerican_Indicator]
				   ,[Race_NativeHawaiianPacificIslander_Indicator]
				   ,[Race_White_Indicator]
				   ,[Race_MultiRace_Indicator]
				   ,[Race_ChooseNotRespond_Indicator]
				   ,[Race_Other_Indicator]
				   ,[EthnicityCode]
				   ,[EthnicityDescription]
				   ,[EthnicityHispanicLatino_Indicator]
				   ,[Migrant_Indicator]
				   ,[Homeless_Indicator]
				   ,[IEP_Indicator]
				   ,[English_Learner_Code_Value]
				   ,[English_Learner_Description]
				   ,[English_Learner_Indicator]
				   ,[Former_English_Learner_Indicator]
				   ,[Never_English_Learner_Indicator]
				   ,[EconomicDisadvantage_Indicator]
				   ,[EntryDate]
				   ,[EntrySchoolYear]
				   ,[EntryCode]
				   ,[ExitWithdrawDate]
				   ,[ExitWithdrawSchoolYear]
				   ,[ExitWithdrawCode]	   

				   
				   ,StudentMainInfoModifiedDate
	               ,StudentSchoolAssociationModifiedDate

				   ,[ValidFrom]
				   ,[ValidTo]
				   ,[IsCurrent])
			   SELECT DISTINCT
						CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),s.StudentNo)) AS [_sourceKey],
						null AS [PrimaryElectronicMailAddress],
						null AS [PrimaryElectronicMailTypeDescriptor_CodeValue],
						null AS [PrimaryElectronicMailTypeDescriptor_Description],

						s.StudentNo AS [StudentUniqueId],       
						s.sasid AS StateId,
       
						dschool.SchoolKey,
						dschool.ShortNameOfInstitution,
						dschool.NameOfInstitution,
						s.Grade as GradeLevelDescriptor_CodeValue,
						s.Grade as GradeLevelDescriptor_Description,

						s.FirstName,
						LEFT(LTRIM(s.MiddleName),1) AS MiddleInitial,
						s.MiddleName,	   
						s.LastName AS LastSurname,
						dbo.Func_ETL_GetFullName(s.FirstName,s.MiddleName,s.LastName) AS FullName,
						s.DOB AS BirthDate,
						DATEDIFF(YEAR, s.DOB, GetDate()) AS StudentAge,
						s.YOG AS GraduationSchoolYear,

						s.Homeroom,
						NULL AS HomeroomTeacher,

						CASE 
							WHEN s.Sex = 'M' THEN 'M'
							WHEN s.Sex = 'F' THEN 'F'
							ELSE 'NS' -- not selected
						END AS SexType_Code,
						CASE 
							WHEN s.Sex = 'M' THEN 'Male'
							WHEN s.Sex = 'F' THEN 'Female'
							ELSE 'Not Selected' -- not selected
						END AS SexType_Description,
						CASE WHEN s.Sex = 'M' THEN 1 ELSE 0 END AS SexType_Male_Indicator,
						CASE WHEN s.Sex = 'F' THEN 1 ELSE 0 END AS SexType_Female_Indicator,
						CASE WHEN s.Sex not in ( 'M','F') THEN 1 ELSE 0 END AS SexType_NotSelected_Indicator, -- NON BINARY

						CASE WHEN sdir.IsNatAmer = 1 THEN 'American Indian - Alaskan Native'
							WHEN sdir.IsAsian = 1 THEN 'Asian'
							WHEN sdir.IsBlack = 1 THEN 'Black - African American'
							WHEN sdir.IsPacIsland = 1 THEN 'Native Hawaiian - Pacific Islander'
							WHEN sdir.IsWhite = 1 THEN 'White'
							WHEN sdir.IsHispanic = 1 THEN 'Hispanic'
							ELSE 'Choose Not Respond'
						END AS RaceCode,
						CASE WHEN sdir.IsNatAmer = 1 THEN 'American Indian - Alaskan Native'
							WHEN sdir.IsAsian = 1 THEN 'Asian'
							WHEN sdir.IsBlack = 1 THEN 'Black - African American'
							WHEN sdir.IsPacIsland = 1 THEN 'Native Hawaiian - Pacific Islander'
							WHEN sdir.IsWhite = 1 THEN 'White'
							WHEN sdir.IsHispanic = 1 THEN 'Hispanic'
							ELSE 'Choose Not Respond'
						END AS RaceDescription,
						CASE WHEN sdir.IsNatAmer = 1 THEN 'American Indian - Alaskan Native'
							WHEN sdir.IsAsian = 1 THEN 'Asian'
							WHEN sdir.IsBlack = 1 THEN 'Black - African American'
							WHEN sdir.IsPacIsland = 1 THEN 'Native Hawaiian - Pacific Islander'
							WHEN sdir.IsWhite = 1 THEN 'White'
							WHEN sdir.IsHispanic = 1 THEN 'Hispanic'
							ELSE 'Choose Not Respond'
						END AS StateRaceCode,
						sdir.IsNatAmer AS Race_AmericanIndianAlaskanNative_Indicator,
						sdir.IsAsian AS Race_Asian_Indicator,
						sdir.IsBlack AS Race_BlackAfricaAmerican_Indicator,
						sdir.IsPacIsland AS Race_NativeHawaiianPacificIslander_Indicator,
						sdir.IsWhite AS Race_White_Indicator,
						0 AS Race_MultiRace_Indicator, 
						CASE WHEN sdir.IsNatAmer = 0 AND
									sdir.IsAsian = 0 AND 
									sdir.IsBlack = 0 AND 
									sdir.IsPacIsland = 0 AND
									sdir.IsWhite = 0 AND 
									sdir.IsHispanic = 0 THEN 1 
									ELSE 0
						END AS Race_ChooseNotRespond_Indicator,
						0 AS Race_Other_Indicator,

						CASE WHEN sdir.IsHispanic = 1 THEN 'H' ELSE 'Non-H' END  AS EthnicityCode,
						CASE WHEN sdir.IsHispanic = 1 THEN 'Hispanic' ELSE 'Non Hispanic' END  AS EthnicityDescription,
						sdir.IsHispanic AS EthnicityHispanicLatino_Indicator,
	   
						0 AS Migrant_Indicator,
						CASE WHEN hsby.studentno IS NULL THEN 0 ELSE 1 END AS Homeless_Indicator,	   
						case WHEN COALESCE(s.SnCode,'None') <> 'None' THEN 1  ELSE 0 END  AS IEP_Indicator,
	   
						COALESCE(s.Lep_Status,'N/A') AS LimitedEnglishProficiencyDescriptor_CodeValue,
						COALESCE(s.Lep_Status,'N/A') AS LimitedEnglishProficiencyDescriptor_Description,
						CASE WHEN COALESCE(s.Lep_Status,'N/A') = 'L' THEN 1 ELSE 0 END [LimitedEnglishProficiency_EnglishLearner_Indicator],
						CASE WHEN COALESCE(s.Lep_Status,'N/A') = 'F' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_Former_Indicator,
						CASE WHEN COALESCE(s.Lep_Status,'N/A') = 'N' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_NotEnglisLearner_Indicator,
						CASE WHEN COALESCE(s.foodgroup,'None') <> 'None' THEN 1 ELSE 0 END AS EconomicDisadvantage_Indicator,
       
						--entry	   
						CASE WHEN MONTH(s.entdate) >= 7 THEN 
								DATEADD(YEAR,s.schyear  - YEAR(s.entdate),s.entdate)
							ELSE 
								DATEADD(YEAR,s.schyear + 1  - YEAR(s.entdate),s.entdate)
						END AS EntryDate,

						s.schyear + 1 AS EntrySchoolYear, 
						COALESCE(s.entcode,'N/A') AS EntryCode,
       
						--exit
						CASE WHEN s.schyearsequenceno =  999999 AND s.withdate IS null   THEN '6/30/' + CAST(s.schyear + 1 AS NVARCHAR(max)) 
							ELSE s.withdate
						END AS ExitWithdrawDate,
						s.schyear + 1 AS ExitWithdrawSchoolYear, 
						COALESCE(s.withcode,'N/A') AS ExitWithdrawCode,
				
						'07/01/2015' AS SchoolCategoryModifiedDate,
						'07/01/2015' AS SchoolTitle1StatusModifiedDate,

						CASE WHEN MONTH(s.entdate) >= 7 THEN 
								DATEADD(YEAR,s.schyear  - YEAR(s.entdate),s.entdate)
							ELSE 
								DATEADD(YEAR,s.schyear + 1  - YEAR(s.entdate),s.entdate)
						END AS ValidFrom,
						CASE WHEN s.schyearsequenceno =  999999 AND s.withdate IS null   THEN '6/30/' + CAST(s.schyear + 1 AS NVARCHAR(max)) 
						      WHEN s.schyearsequenceno <>  999999 AND s.withdate IS null   THEN s.entdate
							  ELSE s.withdate
						END AS ValidTo
						,0 IsCurrent
				--select distinct top 1000 *
				FROM [BPSGranary02].[BPSDW].[dbo].[student] s 
					--WHERE schyear IN (2017,2016,2015) AND s.StudentNo = '210191' ORDER BY s.StudentNo, s.entdate
						INNER JOIN [BPSGranary02].[RAEDatabase].[dbo].[studentdir] sdir ON s.StudentNo = sdir.studentno
						INNER JOIN dbo.DimSchool dschool ON  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),s.sch))  = dschool._sourceKey	 
						LEFT JOIN HomelessStudentsByYear hsby ON s.StudentNo = hsby.studentno 
															and s.schyear = hsby.schyear
				WHERE s.schyear IN (2017,2016,2015)
						and s.sch between '1000' and '4700'
				ORDER BY s.StudentNo;

			END

			

		--COMMIT TRANSACTION;		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		---- Test whether the transaction is uncommittable.
		--IF XACT_STATE( ) = -1
		--	BEGIN
		--		--The transaction is in an uncommittable state. Rolling back transaction
		--		ROLLBACK TRANSACTION;
		--	END;

		---- Test whether the transaction is committable.
		--IF XACT_STATE( ) = 1
		--	BEGIN
		--		--The transaction is committable. Committing transaction
		--		COMMIT TRANSACTION;
		--	END;
	END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimStudent_PopulateProduction]
@LineageKey INT,
@LastDateLoaded DATETIME
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	    
		BEGIN TRANSACTION;   
		
		--empty row technique
		--fact table should not have null foreign keys references
		--this empty record will be used in those cases
		IF NOT EXISTS (SELECT 1 
		               FROM dbo.DimStudent WHERE _sourceKey = '')
				BEGIN
				   INSERT INTO [dbo].DimStudent
				   (
				       _sourceKey,
				       StudentUniqueId,
				       StateId,
				       PrimaryElectronicMailAddress,
				       PrimaryElectronicMailTypeDescriptor_CodeValue,
				       PrimaryElectronicMailTypeDescriptor_Description,
				       SchoolKey,
				       ShortNameOfInstitution,
				       NameOfInstitution,
				       GradeLevelDescriptor_CodeValue,
				       GradeLevelDescriptor_Description,
				       FirstName,
				       MiddleInitial,
				       MiddleName,
				       LastSurname,
				       FullName,
				       BirthDate,
				       StudentAge,
				       GraduationSchoolYear,
				       Homeroom,
				       HomeroomTeacher,
				       SexType_Code,
				       SexType_Description,
				       SexType_Male_Indicator,
				       SexType_Female_Indicator,
				       SexType_NotSelected_Indicator,
				       RaceCode,
				       RaceDescription,
				       StateRaceCode,
				       Race_AmericanIndianAlaskanNative_Indicator,
				       Race_Asian_Indicator,
				       Race_BlackAfricaAmerican_Indicator,
				       Race_NativeHawaiianPacificIslander_Indicator,
				       Race_White_Indicator,
				       Race_MultiRace_Indicator,
				       Race_ChooseNotRespond_Indicator,
				       Race_Other_Indicator,
				       EthnicityCode,
				       EthnicityDescription,
				       EthnicityHispanicLatino_Indicator,
				       Migrant_Indicator,
				       Homeless_Indicator,
				       IEP_Indicator,
				       English_Learner_Code_Value,
				       English_Learner_Description,
				       English_Learner_Indicator,
				       Former_English_Learner_Indicator,
				       Never_English_Learner_Indicator,
				       EconomicDisadvantage_Indicator,
				       EntryDate,
				       EntrySchoolYear,
				       EntryCode,
				       ExitWithdrawDate,
				       ExitWithdrawSchoolYear,
				       ExitWithdrawCode,
				       ValidFrom,
				       ValidTo,
				       IsCurrent,
				       LineageKey
				   )
				   VALUES
				   (   N'',           -- _sourceKey - nvarchar(50)
				       N'N/A',           -- StudentUniqueId - nvarchar(32)
				       N'N/A',           -- StateId - nvarchar(32)
				       N'N/A',           -- PrimaryElectronicMailAddress - nvarchar(128)
				       N'N/A',           -- PrimaryElectronicMailTypeDescriptor_CodeValue - nvarchar(128)
				       N'N/A',           -- PrimaryElectronicMailTypeDescriptor_Description - nvarchar(128)
				       0,             -- SchoolKey - int
				       N'N/A',           -- ShortNameOfInstitution - nvarchar(500)
				       N'N/A',           -- NameOfInstitution - nvarchar(500)
				       N'N/A',           -- GradeLevelDescriptor_CodeValue - nvarchar(100)
				       N'N/A',           -- GradeLevelDescriptor_Description - nvarchar(500)
				       N'N/A',           -- FirstName - nvarchar(100)
				       '',            -- MiddleInitial - char(1)
				       N'N/A',           -- MiddleName - nvarchar(100)
				       N'N/A',           -- LastSurname - nvarchar(100)
				       N'N/A',           -- FullName - nvarchar(500)
				       GETDATE(),     -- BirthDate - date
				       0,             -- StudentAge - int
				       0,             -- GraduationSchoolYear - int
				       N'N/A',           -- Homeroom - nvarchar(500)
				       N'N/A',           -- HomeroomTeacher - nvarchar(500)
				       N'N/A',           -- SexType_Code - nvarchar(100)
				       N'N/A',           -- SexType_Description - nvarchar(100)
				       0,          -- SexType_Male_Indicator - bit
				       0,          -- SexType_Female_Indicator - bit
				       0,          -- SexType_NotSelected_Indicator - bit
				       N'N/A',           -- RaceCode - nvarchar(1000)
				       N'N/A',           -- RaceDescription - nvarchar(1000)
				       N'N/A',           -- StateRaceCode - nvarchar(1000)
				       0,          -- Race_AmericanIndianAlaskanNative_Indicator - bit
				       0,          -- Race_Asian_Indicator - bit
				       0,          -- Race_BlackAfricaAmerican_Indicator - bit
				       0,          -- Race_NativeHawaiianPacificIslander_Indicator - bit
				       0,          -- Race_White_Indicator - bit
				       0,          -- Race_MultiRace_Indicator - bit
				       0,          -- Race_ChooseNotRespond_Indicator - bit
				       0,          -- Race_Other_Indicator - bit
				       N'N/A',           -- EthnicityCode - nvarchar(100)
				       N'N/A',           -- EthnicityDescription - nvarchar(100)
				       0,          -- EthnicityHispanicLatino_Indicator - bit
				       0,          -- Migrant_Indicator - bit
				       0,          -- Homeless_Indicator - bit
				       0,          -- IEP_Indicator - bit
				       N'',           -- English_Learner_Code_Value - nvarchar(100)
				       N'',           -- English_Learner_Description - nvarchar(100)
				       0,          -- English_Learner_Indicator - bit
				       0,          -- Former_English_Learner_Indicator - bit
				       0,          -- Never_English_Learner_Indicator - bit
				       0,          -- EconomicDisadvantage_Indicator - bit
				       SYSDATETIME(), -- EntryDate - datetime2(7)
				       0,             -- EntrySchoolYear - int
				       N'N/A',        -- EntryCode - nvarchar(25)
				       SYSDATETIME(), -- ExitWithdrawDate - datetime2(7)
				       0,           -- ExitWithdrawSchoolYear - int
				       N'N/A',        -- ExitWithdrawCode - nvarchar(100)
				      '07/01/2015', -- ValidFrom - datetime
					  '9999-12-31', -- ValidTo - datetime
					   0,      -- IsCurrent - bit
					   -1          -- LineageKey - int
				       )
				    
				END

		--staging table holds newer records. 
		--the matching prod records will be valid until the date in which the newest data change was identified
		UPDATE prod
		SET prod.ValidTo = stage.ValidFrom,
		    prod.IsCurrent = 0
		FROM 
			[dbo].[DimStudent] AS prod
			INNER JOIN Staging.Student AS stage ON prod._sourceKey = stage._sourceKey
		WHERE prod.ValidTo = '12/31/9999'


		INSERT INTO dbo.DimStudent
		(
		    [_sourceKey]
           ,[PrimaryElectronicMailAddress]
		   ,[PrimaryElectronicMailTypeDescriptor_CodeValue]
		   ,[PrimaryElectronicMailTypeDescriptor_Description]
           ,[StudentUniqueId]
           ,[StateId]
           ,[SchoolKey]
           ,[ShortNameOfInstitution]
           ,[NameOfInstitution]
           ,[GradeLevelDescriptor_CodeValue]
           ,[GradeLevelDescriptor_Description]
           ,[FirstName]
           ,[MiddleInitial]
           ,[MiddleName]
           ,[LastSurname]
           ,[FullName]
           ,[BirthDate]
           ,[StudentAge]
           ,[GraduationSchoolYear]
           ,[Homeroom]
           ,[HomeroomTeacher]
           ,[SexType_Code]
           ,[SexType_Description]
           ,[SexType_Male_Indicator]
           ,[SexType_Female_Indicator]
           ,[SexType_NotSelected_Indicator]
           ,[RaceCode]
           ,[RaceDescription]
		   ,[StateRaceCode]
           ,[Race_AmericanIndianAlaskanNative_Indicator]
           ,[Race_Asian_Indicator]
           ,[Race_BlackAfricaAmerican_Indicator]
           ,[Race_NativeHawaiianPacificIslander_Indicator]
           ,[Race_White_Indicator]
           ,[Race_MultiRace_Indicator]
           ,[Race_ChooseNotRespond_Indicator]
           ,[Race_Other_Indicator]
           ,[EthnicityCode]
           ,[EthnicityDescription]
           ,[EthnicityHispanicLatino_Indicator]
           ,[Migrant_Indicator]
           ,[Homeless_Indicator]
           ,[IEP_Indicator]
           ,[English_Learner_Code_Value]
           ,[English_Learner_Description]
           ,[English_Learner_Indicator]
           ,[Former_English_Learner_Indicator]
           ,[Never_English_Learner_Indicator]
           ,[EconomicDisadvantage_Indicator]
           ,[EntryDate]
           ,[EntrySchoolYear]
           ,[EntryCode]
           ,[ExitWithdrawDate]
           ,[ExitWithdrawSchoolYear]
           ,[ExitWithdrawCode]
           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey]
		)
		SELECT 
		    [_sourceKey]
           ,[PrimaryElectronicMailAddress]
		   ,[PrimaryElectronicMailTypeDescriptor_CodeValue]
		   ,[PrimaryElectronicMailTypeDescriptor_Description]
           ,[StudentUniqueId]
           ,[StateId]
           ,[SchoolKey]
           ,[ShortNameOfInstitution]
           ,[NameOfInstitution]
           ,[GradeLevelDescriptor_CodeValue]
           ,[GradeLevelDescriptor_Description]
           ,[FirstName]
           ,[MiddleInitial]
           ,[MiddleName]
           ,[LastSurname]
           ,[FullName]
           ,[BirthDate]
           ,[StudentAge]
           ,[GraduationSchoolYear]
           ,[Homeroom]
           ,[HomeroomTeacher]
           ,[SexType_Code]
           ,[SexType_Description]
           ,[SexType_Male_Indicator]
           ,[SexType_Female_Indicator]
           ,[SexType_NotSelected_Indicator]
           ,[RaceCode]
           ,[RaceDescription]
		   ,[StateRaceCode]
           ,[Race_AmericanIndianAlaskanNative_Indicator]
           ,[Race_Asian_Indicator]
           ,[Race_BlackAfricaAmerican_Indicator]
           ,[Race_NativeHawaiianPacificIslander_Indicator]
           ,[Race_White_Indicator]
           ,[Race_MultiRace_Indicator]
           ,[Race_ChooseNotRespond_Indicator]
           ,[Race_Other_Indicator]
           ,[EthnicityCode]
           ,[EthnicityDescription]
           ,[EthnicityHispanicLatino_Indicator]
           ,[Migrant_Indicator]
           ,[Homeless_Indicator]
           ,[IEP_Indicator]
           ,[English_Learner_Code_Value]
           ,[English_Learner_Description]
           ,[English_Learner_Indicator]
           ,[Former_English_Learner_Indicator]
           ,[Never_English_Learner_Indicator]
           ,[EconomicDisadvantage_Indicator]
           ,[EntryDate]
           ,[EntrySchoolYear]
           ,[EntryCode]
           ,[ExitWithdrawDate]
           ,[ExitWithdrawSchoolYear]
           ,[ExitWithdrawCode]
           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]           
		   ,@LineageKey
		FROM Staging.Student

		--BPS is having data quality issues with exit/withdrawal dates in the ODS
		--this is patch to overcome the problem and maintain the data clean momentarily
		--------------------------------------------------------------------------------------------------
		UPDATE dbo.DimStudent
		SET IsCurrent = 0;

		DECLARE @currentSchoolYear INT 
		SELECT TOP (1) @currentSchoolYear =  SchoolYear 
		FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolYearType syt
		WHERE syt.CurrentSchoolYear = 1;
		

		;WITH LatestEntryDatePerStudent AS
        (
			SELECT DISTINCT 
				   ds.StudentUniqueId, 
				   ds.StudentKey, 
				   ROW_NUMBER() OVER (PARTITION BY ds.StudentUniqueId ORDER BY ds.EntryDate DESC) AS RowRankId
			FROM dbo.DimStudent ds 
			WHERE ds.EntrySchoolYear = @currentSchoolYear
		)

		UPDATE ds 
		SET ds.IsCurrent = 1
		FROM dbo.DimStudent ds 
		WHERE ds.EntrySchoolYear = @currentSchoolYear
		      AND EXISTS (SELECT 1 
			              FROM LatestEntryDatePerStudent ledps 
						  WHERE ds.StudentKey = ledps.StudentKey 
						    AND ledps.RowRankId = 1);
		---------------------------------------------------------------------------------------------------


		-- updating the EndTime to now and status to Success		
		UPDATE dbo.ETL_Lineage
			SET 
				EndTime = SYSDATETIME(),
				Status = 'S' -- success
		WHERE [LineageKey] = @LineageKey;
	
	
		-- Update the LoadDates table with the most current load date
		UPDATE [dbo].[ETL_IncrementalLoads]
		SET [LoadDate] = @LastDateLoaded
		WHERE [TableName] = N'dbo.DimStudent';

		COMMIT TRANSACTION;		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		IF XACT_STATE( ) = -1
			BEGIN
				--The transaction is in an uncommittable state. Rolling back transaction
				ROLLBACK TRANSACTION;
			END;

		-- Test whether the transaction is committable.
		IF XACT_STATE( ) = 1
			BEGIN
				--The transaction is committable. Committing transaction
				COMMIT TRANSACTION;
			END;
	END CATCH;
END;
GO

--Dim AttendanceEventCategory
--------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimAttendanceEventCategory_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY

		TRUNCATE TABLE Staging.AttendanceEventCategory
		INSERT INTO Staging.AttendanceEventCategory
				   ([_sourceKey]
					,[AttendanceEventCategoryDescriptor_CodeValue]
					,[AttendanceEventCategoryDescriptor_Description]
					,[InAttendance_Indicator]
					,[UnexcusedAbsence_Indicator]
					,[ExcusedAbsence_Indicator]
					,[Tardy_Indicator]
					,[EarlyDeparture_Indicator]
					,[CategoryModifiedDate]
					,[ValidFrom]
					,[ValidTo]
					,[IsCurrent])
        --declare @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate()
		SELECT  DISTINCT 
			    CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),d.DescriptorId)) AS [_sourceKey],
				COALESCE(d.CodeValue,'In Attendance') as AttendanceEventCategoryDescriptor_CodeValue,
				COALESCE(d.CodeValue,'In Attendance') as AttendanceEventCategoryDescriptor_Description,
				case when COALESCE(d.CodeValue,'In Attendance') in ('In Attendance','Tardy','Early departure') then 1 else 0 end as [InAttendance_Indicator], -- not used
				case when COALESCE(d.CodeValue,'In Attendance') in ('Unexcused Absence') then 1 else 0 end as [UnexcusedAbsence_Indicator],
				case when COALESCE(d.CodeValue,'In Attendance') in ('Excused Absence') then 1 else 0 end as [ExcusedAbsence_Indicator],
				case when COALESCE(d.CodeValue,'In Attendance') in ('Tardy') then 1 else 0 end as [Tardy_Indicator],	   
				case when COALESCE(d.CodeValue,'In Attendance') in ('Early departure') then 1 else 0 end as [EarlyDeparture_Indicator],	  
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(d.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS [CategoryModifiedDate],
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN
				           (SELECT MAX(t) FROM
                             (VALUES
                               (d.LastModifiedDate)                             
                             ) AS [MaxLastModifiedDate](t)
                           )
					ELSE 
					      '07/01/2015' -- setting the validFrom to beggining of time during thre first load. 
				END AS ValidFrom,
				'12/31/9999' AS ValidTo,
				1  AS IsCurrent				
		--select *  
		FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d
		WHERE d.Namespace IN ('http://ed-fi.org/Descriptor/AttendanceEventCategoryDescriptor.xml',
		                      'http://ed-fi.org/Descriptor/Follett/Aspen/AttendanceEventCategoryDescriptor.xml')	
			  AND (d.LastModifiedDate > @LastLoadDate AND d.LastModifiedDate <= @NewLoadDate);
		
			
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		
	END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimAttendanceEventCategory_PopulateProduction]
@LineageKey INT,
@LastDateLoaded DATETIME
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	    
		BEGIN TRANSACTION;   
				 
	     
		--empty row technique
		--fact table should not have null foreign keys references
		--this empty record will be used in those cases
		IF NOT EXISTS (SELECT 1 
		               FROM dbo.DimAttendanceEventCategory WHERE _sourceKey = '')
				BEGIN
				   INSERT INTO dbo.DimAttendanceEventCategory
				   (
				       _sourceKey,
				       AttendanceEventCategoryDescriptor_CodeValue,
				       AttendanceEventCategoryDescriptor_Description,
				       InAttendance_Indicator,
				       UnexcusedAbsence_Indicator,
				       ExcusedAbsence_Indicator,
				       Tardy_Indicator,
				       EarlyDeparture_Indicator,
				       ValidFrom,
				       ValidTo,
				       IsCurrent,
				       LineageKey
				   )
				   VALUES
				   (   N'',       -- _sourceKey - nvarchar(50)
				       N'N/A',       -- AttendanceEventCategoryDescriptor_CodeValue - nvarchar(50)
				       N'N/A',       -- AttendanceEventCategoryDescriptor_Description - nvarchar(1024)
				       0,      -- InAttendance_Indicator - bit
				       0,      -- UnexcusedAbsence_Indicator - bit
				       0,      -- ExcusedAbsence_Indicator - bit
				       0,      -- Tardy_Indicator - bit
				       0,      -- EarlyDeparture_Indicator - bit
				       '07/01/2015', -- ValidFrom - datetime
					   '9999-12-31', -- ValidTo - datetime
					   0,      -- IsCurrent - bit
					   -1          -- LineageKey - int
				     )
							  
				END

		
		--staging table holds newer records. 
		--the matching prod records will be valid until the date in which the newest data change was identified
		UPDATE prod
		SET prod.ValidTo = stage.ValidFrom
		FROM 
			[dbo].[DimAttendanceEventCategory] AS prod
			INNER JOIN Staging.AttendanceEventCategory AS stage ON prod._sourceKey = stage._sourceKey
		WHERE prod.ValidTo = '12/31/9999'

		
           
		INSERT INTO dbo.DimAttendanceEventCategory
           ([_sourceKey]
           ,[AttendanceEventCategoryDescriptor_CodeValue]
           ,[AttendanceEventCategoryDescriptor_Description]
           ,[InAttendance_Indicator]
           ,[UnexcusedAbsence_Indicator]
           ,[ExcusedAbsence_Indicator]
           ,[Tardy_Indicator]
           ,[EarlyDeparture_Indicator]
           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])
		SELECT 
		    [_sourceKey]
           ,[AttendanceEventCategoryDescriptor_CodeValue]
           ,[AttendanceEventCategoryDescriptor_Description]
           ,[InAttendance_Indicator]
           ,[UnexcusedAbsence_Indicator]
           ,[ExcusedAbsence_Indicator]
           ,[Tardy_Indicator]
           ,[EarlyDeparture_Indicator]
           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
		   ,@LineageKey
		FROM Staging.AttendanceEventCategory

		-- updating the EndTime to now and status to Success		
		UPDATE dbo.ETL_Lineage
			SET 
				EndTime = SYSDATETIME(),
				Status = 'S' -- success
		WHERE [LineageKey] = @LineageKey;
	
	
		-- Update the LoadDates table with the most current load date
		UPDATE [dbo].[ETL_IncrementalLoads]
		SET [LoadDate] = @LastDateLoaded
		WHERE [TableName] = N'dbo.DimAttendanceEventCategory';

		COMMIT TRANSACTION;		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		IF XACT_STATE( ) = -1
			BEGIN
				--The transaction is in an uncommittable state. Rolling back transaction
				ROLLBACK TRANSACTION;
			END;

		-- Test whether the transaction is committable.
		IF XACT_STATE( ) = 1
			BEGIN
				--The transaction is committable. Committing transaction
				COMMIT TRANSACTION;
			END;
	END CATCH;
END;
GO

--Dim DisciplineIncident
--------------------------------------------------------------------
--[dbo].[Proc_ETL_DimDisciplineIncident_PopulateStaging]  '2021-02-08 01:01:56.000' , '02/17/2021'
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimDisciplineIncident_PopulateStaging]  
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	--SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

	BEGIN TRY
		--declare @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate()	
		TRUNCATE TABLE Staging.DisciplineIncident
		INSERT INTO Staging.DisciplineIncident
				   (_sourceKey
				    ,[SchoolKey]
				    ,[ShortNameOfInstitution]
				    ,[NameOfInstitution]
				    ,[SchoolYear]
				    ,[IncidentDate]
				    ,[IncidentTime]
				    ,[IncidentDescription]
				    ,[BehaviorDescriptor_CodeValue]
				    ,[BehaviorDescriptor_Description]
				    ,[LocationDescriptor_CodeValue]
				    ,[LocationDescriptor_Description]
				    ,[DisciplineDescriptor_CodeValue]
				    ,[DisciplineDescriptor_Description]
				    ,DisciplineDescriptor_ISS_Indicator
				    ,DisciplineDescriptor_OSS_Indicator
				    ,[ReporterDescriptor_CodeValue]
				    ,[ReporterDescriptor_Description]
			 	    
				    ,[IncidentReporterName]
				    ,[ReportedToLawEnforcement_Indicator]
				    ,[IncidentCost]

					,IncidentModifiedDate

				    ,[ValidFrom]
				    ,[ValidTo]
				    ,[IsCurrent])
        
		SELECT DISTINCT 
				CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),di.IncidentIdentifier)) AS [_sourceKey],
				dschool.SchoolKey,
				dschool.ShortNameOfInstitution,
				dschool.NameOfInstitution,
				dbo.Func_ETL_GetSchoolYear(di.IncidentDate),
				di.IncidentDate,
				COALESCE(di.IncidentTime,'00:00:00.0000000') AS IncidentTime,
				di.IncidentDescription,
				COALESCE(d_dib.CodeValue,'N/A') as [BehaviorDescriptor_CodeValue],
				COALESCE(d_dib.Description,'N/A') as [BehaviorDescriptor_Description],
	  
				COALESCE(d_dil.CodeValue,'N/A') as [LocationDescriptor_CodeValue],
				COALESCE(d_dil.Description,'N/A') as [LocationDescriptor_Description],

				COALESCE(d_dia.CodeValue,'N/A') as [DisciplineDescriptor_CodeValue],
				COALESCE(d_dia.Description,'N/A') as [DisciplineDescriptor_Description],
				CASE WHEN  COALESCE(d_dia.CodeValue,'N/A') IN ('In School Suspension','In-School Suspension') THEN 1 ELSE 0 END as DisciplineDescriptor_ISS_Indicator,
				CASE WHEN  COALESCE(d_dia.CodeValue,'N/A') IN ('Out of School Suspension','Out-Of-School Suspension') THEN 1 ELSE 0 END as DisciplineDescriptor_OSS_Indicator,
	  
				COALESCE(d_dirt.CodeValue,'N/A') as ReporterDescriptor_CodeValue,
				COALESCE(d_dirt.Description,'N/A') as ReporterDescriptor_Description,

				COALESCE(di.ReporterName,'N/A'),
				COALESCE(di.ReportedToLawEnforcement,0) AS ReportedToLawEnforcement,
				COALESCE(di.IncidentCost,0) AS IncidentCost,
				
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(di.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS IncidentModifiedDate,

				--Making sure the first time, the ValidFrom is set to beginning of time 
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN
				           (SELECT MAX(t) FROM
                             (VALUES
                               (di.LastModifiedDate)                             
                             ) AS [MaxLastModifiedDate](t)
                           )
					ELSE 
					      '07/01/2015' -- setting the validFrom to beggining of time during thre first load. 
				END AS ValidFrom,
				'12/31/9999' AS ValidTo,
				1 AS IsCurrent		
		FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncident di       
				LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncidentBehavior dib ON di.IncidentIdentifier = dib.IncidentIdentifier
				LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineActionDisciplineIncident dadi ON di.IncidentIdentifier = dadi.IncidentIdentifier
				LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineActionDiscipline dad ON dadi.DisciplineActionIdentifier = dad.DisciplineActionIdentifier

				INNER JOIN dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),di.SchoolId)   = dschool._sourceKey
				INNER JOIN dbo.DimTime dt ON di.IncidentDate = dt.SchoolDate
												AND dt.SchoolKey is not null   
												AND dschool.SchoolKey = dt.SchoolKey
				LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d_dib ON dib.BehaviorDescriptorId   = d_dib.DescriptorId
				LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.IncidentLocationType d_dil ON di.IncidentLocationTypeId   = d_dil.IncidentLocationTypeId
				LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d_dia ON dad.DisciplineDescriptorId   = d_dia.DescriptorId
				LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d_dirt ON di.ReporterDescriptionDescriptorId   = d_dirt.DescriptorId
		WHERE dbo.Func_ETL_GetSchoolYear(di.IncidentDate) >= 2019 AND
		    (
			  	(di.LastModifiedDate > @LastLoadDate AND di.LastModifiedDate <= @NewLoadDate)
			)
													
			
		--loading legacy data if it has not been loaded.
		--load types are ignored as this data will only be loaded once.
		IF NOT EXISTS(SELECT 1 
		              FROM dbo.DimDisciplineIncident 
					  WHERE CHARINDEX('LegacyDW',_sourceKey,1) > 0)
			BEGIN
			   INSERT INTO Staging.DisciplineIncident
				   (_sourceKey
				    ,[SchoolKey]
				    ,[ShortNameOfInstitution]
				    ,[NameOfInstitution]
				    ,[SchoolYear]
				    ,[IncidentDate]
				    ,[IncidentTime]
				    ,[IncidentDescription]
				    ,[BehaviorDescriptor_CodeValue]
				    ,[BehaviorDescriptor_Description]
				    ,[LocationDescriptor_CodeValue]
				    ,[LocationDescriptor_Description]
				    ,[DisciplineDescriptor_CodeValue]
				    ,[DisciplineDescriptor_Description]
				    ,DisciplineDescriptor_ISS_Indicator
				    ,DisciplineDescriptor_OSS_Indicator
				    ,[ReporterDescriptor_CodeValue]
				    ,[ReporterDescriptor_Description]
			 	    
				    ,[IncidentReporterName]
				    ,[ReportedToLawEnforcement_Indicator]
				    ,[IncidentCost]

					,IncidentModifiedDate

				    ,[ValidFrom]
				    ,[ValidTo]
				    ,[IsCurrent])
			 SELECT DISTINCT 
					  CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),di.[CND_INCIDENT_ID])) AS [_sourceKey],    
					  dschool.SchoolKey,
					  dschool.ShortNameOfInstitution,
					  dschool.NameOfInstitution,
					  dbo.Func_ETL_GetSchoolYear(di.[CND_INCIDENT_DATE]) AS [SchoolYear],
					  di.CND_INCIDENT_DATE AS [IncidentDate],
					 -- TRY_CAST(di.CND_INCIDENT_TIME AS DATETIME2) ,
					  CONVERT(char(12),TRY_CAST(di.CND_INCIDENT_TIME AS DATETIME2), 108) IncidentTime,
					  --'00:00:00.0000000' AS ,
					  di.[CND_INCIDENT_DESCRIPTION] AS [IncidentDescription],
					  COALESCE(di.CND_INCIDENT_CODE,'N/A') as [BehaviorDescriptor_CodeValue],
					  COALESCE(di.CND_INCIDENT_CODE,'N/A') as [BehaviorDescriptor_Description],
	  
					  COALESCE(di.[CND_INCIDENT_LOCATION],'N/A') as [LocationDescriptor_CodeValue],
					  COALESCE(di.[CND_INCIDENT_LOCATION],'N/A') as [LocationDescriptor_Description],

					  COALESCE(di.[ACT_ACTION_CODE],'N/A') as [DisciplineDescriptor_CodeValue],
					  COALESCE(di.[ACT_ACTION_CODE],'N/A') as [DisciplineDescriptor_Description],
					  CASE WHEN  COALESCE(di.ACT_ACTION_CODE,'N/A') IN ('In-School Suspension)') THEN 1 ELSE 0 END,
					  CASE WHEN  COALESCE(di.ACT_ACTION_CODE,'N/A') IN ('Out of School Suspension') THEN 1 ELSE 0 END,
	  
					  'N/A' as ReporterDescriptor_CodeValue,
					  'N/A' as ReporterDescriptor_Description,

					  'N/A' AS [IncidentReporterName],
					  0 AS ReportedToLawEnforcement,
					  0 AS IncidentCost,

					  '07/01/2015' AS IncidentModifiedDate,
					  
					  '07/01/2015' AS ValidFrom,
					  GETDATE() AS ValidTo,
					  0 AS IsCurrent		
					  
				--select distinct *
				FROM  [Raw_LegacyDW].[DisciplineIncidents] di
					  INNER JOIN dbo.DimSchool dschool ON CONCAT_WS('|', 'Ed-Fi', Convert(NVARCHAR(MAX),di.[SKL_SCHOOL_ID]))   = dschool._sourceKey 
					  INNER JOIN dbo.DimTime dt ON di.CND_INCIDENT_DATE = dt.SchoolDate
														 AND dt.SchoolKey is not null   
														 AND dschool.SchoolKey = dt.SchoolKey	
				WHERE TRY_CAST(di.CND_INCIDENT_DATE AS DATETIME)  BETWEEN '2015-09-01' AND '2018-06-30'


			END

			
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		
	END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimDisciplineIncident_PopulateProduction]
@LineageKey INT,
@LastDateLoaded DATETIME
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	    
		BEGIN TRANSACTION;   
		 
		--empty row technique
		--fact table should not have null foreign keys references
		--this empty record will be used in those cases
		IF NOT EXISTS (SELECT 1 
		               FROM dbo.DimDisciplineIncident WHERE _sourceKey = '')
				BEGIN
				   INSERT INTO dbo.DimDisciplineIncident
				   (
				       _sourceKey,
				       SchoolKey,
				       ShortNameOfInstitution,
				       NameOfInstitution,
				       SchoolYear,
				       IncidentDate,
				       IncidentTime,
				       IncidentDescription,
				       BehaviorDescriptor_CodeValue,
				       BehaviorDescriptor_Description,
				       LocationDescriptor_CodeValue,
				       LocationDescriptor_Description,
				       DisciplineDescriptor_CodeValue,
				       DisciplineDescriptor_Description,
				       DisciplineDescriptor_ISS_Indicator,
				       DisciplineDescriptor_OSS_Indicator,
				       ReporterDescriptor_CodeValue,
				       ReporterDescriptor_Description,
				       IncidentReporterName,
				       ReportedToLawEnforcement_Indicator,
				       IncidentCost,
					   ValidFrom,
					   ValidTo,
					   IsCurrent,
				       LineageKey
				   )
				   VALUES
				   (   N'',        -- _sourceKey - nvarchar(50)
				       -1,          -- SchoolKey - int
				       N'',        -- ShortNameOfInstitution - nvarchar(500)
				       N'',        -- NameOfInstitution - nvarchar(500)
				       -1,          -- SchoolYear - int
				       GETDATE(),  -- IncidentDate - date
				       '10:50:24', -- IncidentTime - time(7)
				       N'N/A',        -- IncidentDescription - nvarchar(max)
				       N'N/A',        -- BehaviorDescriptor_CodeValue - nvarchar(50)
				       N'N/A',        -- BehaviorDescriptor_Description - nvarchar(1024)
				       N'N/A',        -- LocationDescriptor_CodeValue - nvarchar(50)
				       N'N/A',        -- LocationDescriptor_Description - nvarchar(1024)
				       N'N/A',        -- DisciplineDescriptor_CodeValue - nvarchar(50)
				       N'N/A',        -- DisciplineDescriptor_Description - nvarchar(1024)
				       0,       -- DisciplineDescriptor_ISS_Indicator - bit
				       0,       -- DisciplineDescriptor_OSS_Indicator - bit
				       N'N/A',        -- ReporterDescriptor_CodeValue - nvarchar(50)
				       N'N/A',        -- ReporterDescriptor_Description - nvarchar(1024)
				       N'N/A',        -- IncidentReporterName - nvarchar(100)
				       0,       -- ReportedToLawEnforcement_Indicator - bit
				       0,       -- IncidentCost - money
				       '07/01/2015', -- ValidFrom - datetime
					   '9999-12-31', -- ValidTo - datetime
					   0,      -- IsCurrent - bit
					   -1          -- LineageKey - int
				       )
				  
				END

		--staging table holds newer records. 
		--the matching prod records will be valid until the date in which the newest data change was identified		
		UPDATE prod
		SET prod.ValidTo = stage.ValidFrom
		FROM 
			[dbo].[DimDisciplineIncident] AS prod
			INNER JOIN Staging.DisciplineIncident AS stage ON prod._sourceKey = stage._sourceKey
		WHERE prod.ValidTo = '12/31/9999'


		INSERT INTO dbo.DimDisciplineIncident
		(
		    _sourceKey,
		    SchoolKey,
		    ShortNameOfInstitution,
		    NameOfInstitution,
		    SchoolYear,
		    IncidentDate,
		    IncidentTime,
		    IncidentDescription,
		    BehaviorDescriptor_CodeValue,
		    BehaviorDescriptor_Description,
		    LocationDescriptor_CodeValue,
		    LocationDescriptor_Description,
		    DisciplineDescriptor_CodeValue,
		    DisciplineDescriptor_Description,
		    DisciplineDescriptor_ISS_Indicator,
		    DisciplineDescriptor_OSS_Indicator,
		    ReporterDescriptor_CodeValue,
		    ReporterDescriptor_Description,
		    IncidentReporterName,
		    ReportedToLawEnforcement_Indicator,
		    IncidentCost,
		    [ValidFrom],
		    [ValidTo],
		    [IsCurrent],
			LineageKey
		)
		
		SELECT 
		    _sourceKey,
		    SchoolKey,
		    ShortNameOfInstitution,
		    NameOfInstitution,
		    SchoolYear,
		    IncidentDate,
		    IncidentTime,
		    IncidentDescription,
		    BehaviorDescriptor_CodeValue,
		    BehaviorDescriptor_Description,
		    LocationDescriptor_CodeValue,
		    LocationDescriptor_Description,
		    DisciplineDescriptor_CodeValue,
		    DisciplineDescriptor_Description,
		    DisciplineDescriptor_ISS_Indicator,
		    DisciplineDescriptor_OSS_Indicator,
		    ReporterDescriptor_CodeValue,
		    ReporterDescriptor_Description,
		    IncidentReporterName,
		    ReportedToLawEnforcement_Indicator,
		    IncidentCost,
			[ValidFrom],
		    [ValidTo],
		    [IsCurrent],
		    @LineageKey
		FROM Staging.DisciplineIncident

		-- updating the EndTime to now and status to Success		
		UPDATE dbo.ETL_Lineage
			SET 
				EndTime = SYSDATETIME(),
				Status = 'S' -- success
		WHERE [LineageKey] = @LineageKey;
	
	
		-- Update the LoadDates table with the most current load date
		UPDATE [dbo].[ETL_IncrementalLoads]
		SET [LoadDate] = @LastDateLoaded
		WHERE [TableName] = N'dbo.DimDisciplineIncident';

		COMMIT TRANSACTION;		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		IF XACT_STATE( ) = -1
			BEGIN
				--The transaction is in an uncommittable state. Rolling back transaction
				ROLLBACK TRANSACTION;
			END;

		-- Test whether the transaction is committable.
		IF XACT_STATE( ) = 1
			BEGIN
				--The transaction is committable. Committing transaction
				COMMIT TRANSACTION;
			END;
	END CATCH;
END;
GO

--Dim Assessment
--------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimAssessment_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY

			
		DECLARE @Assessment TABLE
		(   
			AssessmentCategoryDescriptor_CodeValue NVARCHAR(50) NOT NULL,    
			AssessmentCategoryDescriptor_Description NVARCHAR(1024) NOT NULL,    
			AssessmentFamilyTitle NVARCHAR(100) NULL,    	
			AdaptiveAssessment_Indicator bit NOT NULL, 
			AssessmentIdentifier NVARCHAR(60) NOT NULL,   
			ObjectiveAssessmentIdentificationCode NVARCHAR(60) NOT NULL,   
			AssessmentTitle NVARCHAR(500) NOT NULL,

			ReportingMethodDescriptor_CodeValue NVARCHAR(50) NOT NULL,   
			ReportingMethodDescriptor_Description NVARCHAR(1024) NOT NULL,   
	
			ResultDatatypeTypeDescriptor_CodeValue  NVARCHAR(50) NOT NULL,   
			ResultDatatypeTypeDescriptor_Description NVARCHAR(1024) NOT NULL,   


			AssessmentScore_Indicator  BIT NOT NULL,
			AssessmentPerformanceLevel_Indicator  BIT NOT NULL,

			ObjectiveAssessmentScore_Indicator  BIT NOT NULL,
			ObjectiveAssessmentPerformanceLevel_Indicator  BIT NOT NULL,

			AssessmentModifiedDate [datetime] NOT NULL,

			ValidFrom DATETIME NOT NULL, 
			ValidTo DATETIME NOT NULL, 
			IsCurrent BIT NOT NULL,
			IsLegacy BIT NOT NULL

		);



		--Assessmnent
		INSERT INTO @Assessment
		(
			AssessmentCategoryDescriptor_CodeValue,
			AssessmentCategoryDescriptor_Description,
			AssessmentFamilyTitle,
			AdaptiveAssessment_Indicator,
			AssessmentIdentifier,
			ObjectiveAssessmentIdentificationCode,
			AssessmentTitle,

			ReportingMethodDescriptor_CodeValue,   
			ReportingMethodDescriptor_Description,   
	
			ResultDatatypeTypeDescriptor_CodeValue,   
			ResultDatatypeTypeDescriptor_Description,   


			AssessmentScore_Indicator,
			AssessmentPerformanceLevel_Indicator,

			ObjectiveAssessmentScore_Indicator,
			ObjectiveAssessmentPerformanceLevel_Indicator,

			AssessmentModifiedDate,
			ValidFrom, 
			ValidTo, 
			IsCurrent,
			IsLegacy
		)

		SELECT DISTINCT 
			   a_d.CodeValue AS [AssessmentCategoryDescriptor_CodeValue],
			   a_d.[Description] AS [AssessmentCategoryDescriptor_Description],
			   a.AssessmentFamilyTitle AS [AssessmentFamilyTitle],
			   ISNULL(a.AdaptiveAssessment,0) AS [AdaptiveAssessment_Indicator], 
			   a.AssessmentIdentifier,
			   'N/A' AS ObjectiveAssessmentIdentificationCode,
			   a.AssessmentTitle,	   

			   a_s_armt.CodeValue AS ReportingMethodDescriptor_CodeValue,
			   a_s_armt.[Description] AS ReportingMethodDescriptor_Description,
			   a_s_rdtt.CodeValue AS ResultDatatypeTypeDescriptor_CodeValue,
			   a_s_rdtt.[Description] AS ResultDatatypeTypeDescriptor_Description,

			   1 AS AssessmentScore_Indicator,
			   0 AS AssessmentPerformanceLevel_Indicator,
  
			   0 AS ObjectiveAssessmentScore_Indicator,
			   0 AS ObjectiveAssessmentPerformanceLevel_Indicator,
			   
			   CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(a.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS AssessmentModifiedDate,
			   --Making sure the first time, the ValidFrom is set to beginning of time 
			   CASE WHEN @LastLoadDate <> '07/01/2015' THEN
				           (SELECT MAX(t) FROM
                             (VALUES
                               (a.LastModifiedDate)                             
                             ) AS [MaxLastModifiedDate](t)
                           )
					ELSE 
					      '07/01/2015' -- setting the validFrom to beggining of time during thre first load. 
			   END AS ValidFrom,
			   '12/31/9999' AS ValidTo,
			   1 AS IsCurrent,
			   0 AS IsLegacy
		FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Assessment a 
			 INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor a_d ON a.AssessmentCategoryDescriptorId = a_d.DescriptorId
			 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.AssessmentScore a_s ON a.AssessmentIdentifier = a_s.AssessmentIdentifier 
			 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.AssessmentReportingMethodType a_s_armt ON a_s.AssessmentReportingMethodTypeId = a_s_armt.AssessmentReportingMethodTypeId
			 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ResultDatatypeType a_s_rdtt ON a_s.ResultDatatypeTypeId = a_s_rdtt.ResultDatatypeTypeId
		WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1 
		      AND (a.LastModifiedDate > @LastLoadDate AND a.LastModifiedDate <= @NewLoadDate)
		Union
		SELECT DISTINCT 
			   a_d.CodeValue AS [AssessmentCategoryDescriptor_CodeValue],
			   a_d.[Description] AS [AssessmentCategoryDescriptor_Description],
			   a.AssessmentFamilyTitle AS [AssessmentFamilyTitle],
			   ISNULL(a.AdaptiveAssessment,0) AS [AdaptiveAssessment_Indicator], 
			   a.AssessmentIdentifier,
				'N/A' AS ObjectiveAssessmentIdentificationCode,
			   a.AssessmentTitle,	   
	   
			   a_pl_armt.CodeValue AS ReportingMethodDescriptor_CodeValue,
			   a_pl_armt.[Description] AS ReportingMethodDescriptor_Description,
			   a_pl_rdtt.CodeValue AS ResultDatatypeTypeDescriptor_CodeValue,
			   a_pl_rdtt.[Description] AS ResultDatatypeTypeDescriptor_Description,

			   0 AS AssessmentScore_Indicator,
			   1 AS AssessmentPerformanceLevel_Indicator,
  
			   0 AS ObjectiveAssessmentScore_Indicator,
			   0 AS ObjectiveAssessmentPerformanceLevel_Indicator,
			   
			   CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(a.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS AssessmentModifiedDate,
			   ---Making sure the first time, the ValidFrom is set to beginning of time 
			   CASE WHEN @LastLoadDate <> '07/01/2015' THEN
				           (SELECT MAX(t) FROM
                             (VALUES
                               (a.LastModifiedDate)                             
                             ) AS [MaxLastModifiedDate](t)
                           )
					ELSE 
					      '07/01/2015' -- setting the validFrom to beggining of time during thre first load. 
			   END AS ValidFrom,
			   '12/31/9999' AS ValidTo,
			   1 AS IsCurrent,
			   0 AS IsLegacy
		FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Assessment a 
			 INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor a_d ON a.AssessmentCategoryDescriptorId = a_d.DescriptorId
	 
	 
			 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.AssessmentPerformanceLevel a_pl ON a.AssessmentIdentifier = a_pl.AssessmentIdentifier 
			 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor a_pl_d ON a_pl.PerformanceLevelDescriptorId = a_pl_d.DescriptorId
			 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.AssessmentReportingMethodType a_pl_armt ON a_pl.AssessmentReportingMethodTypeId = a_pl_armt.AssessmentReportingMethodTypeId
			 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ResultDatatypeType a_pl_rdtt ON a_pl.ResultDatatypeTypeId = a_pl_rdtt.ResultDatatypeTypeId
		WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1 
		      AND (a.LastModifiedDate > @LastLoadDate AND a.LastModifiedDate <= @NewLoadDate)
		--ORDER BY a.AssessmentIdentifier, ObjectiveAssessmentIdentificationCode, ReportingMethodDescriptor_CodeValue

		
			
							
			
		--loading legacy data if it has not been loaded.
		--load types are ignored as this data will only be loaded once.
		IF NOT EXISTS(SELECT 1 
		              FROM dbo.DimAssessment 
					  WHERE CHARINDEX('LegacyDW',_sourceKey,1) > 0)
			BEGIN
			   ;WITH UnpivotedScores AS 
				(
					SELECT testid,schyear,testtime,studentno,adminyear,grade, scoretype, scorevalue, CASE WHEN a.scoretype IN ('Proficiency level','Proficiency level 2') THEN 1 ELSE 0 END AS isperflevel
					--INTO [Raw_LegacyDW].[MCASAssessmentScores]
					FROM (  
							 --ensuring all score columns have the same data type to avoid conflicts with unpivot
							 SELECT testid,schyear,testtime,studentno,adminyear,grade,teststatus ,
								  CAST(rawscore AS NVARCHAR(MAX)) AS [Raw score],
								  CAST(scaledscore AS NVARCHAR(MAX)) AS [Scale score],
								  CAST(perflevel AS NVARCHAR(MAX)) AS  [Proficiency level],
								  CAST(sgp AS NVARCHAR(MAX)) AS [Percentile rank],
								  CAST(cpi AS NVARCHAR(MAX)) AS [Composite Performance Index],
								  CAST(perf2 AS NVARCHAR(MAX)) AS [Proficiency level 2]
     
							FROM [BPSGranary02].[RAEDatabase].[dbo].[mcasitems] 
							WHERE schyear in (2015,2016,2017) ) scores
					UNPIVOT
					(
					   scorevalue
					   FOR scoretype IN ([Raw score],[Scale score],[Proficiency level],[Percentile rank],[Composite Performance Index],[Proficiency level 2])
					) AS a
				)

			   INSERT INTO @Assessment
				(
					AssessmentCategoryDescriptor_CodeValue,
					AssessmentCategoryDescriptor_Description,
					AssessmentFamilyTitle,
					AdaptiveAssessment_Indicator,
					AssessmentIdentifier,
					ObjectiveAssessmentIdentificationCode,
					AssessmentTitle,

					ReportingMethodDescriptor_CodeValue,   
					ReportingMethodDescriptor_Description,   
	
					ResultDatatypeTypeDescriptor_CodeValue,   
					ResultDatatypeTypeDescriptor_Description,   


					AssessmentScore_Indicator,
					AssessmentPerformanceLevel_Indicator,

					ObjectiveAssessmentScore_Indicator,
					ObjectiveAssessmentPerformanceLevel_Indicator,

					AssessmentModifiedDate,
					ValidFrom, 
					ValidTo, 
					IsCurrent,
			        IsLegacy
				)

				SELECT DISTINCT 
					   'State assessment' AS [AssessmentCategoryDescriptor_CodeValue],
					   'State assessment' AS [AssessmentCategoryDescriptor_Description],
					   NULL AS [AssessmentFamilyTitle],
					   0 AS [AdaptiveAssessment_Indicator], 
					   testid AS AssessmentIdentifier,
					   'N/A' AS ObjectiveAssessmentIdentificationCode,
					   testid AS AssessmentTitle,	   

					   scoretype AS ReportingMethodDescriptor_CodeValue,
					   scoretype AS ReportingMethodDescriptor_Description,
					   CASE WHEN isperflevel = 1 THEN 'Level' ELSE 'Integer' end   AS ResultDatatypeTypeDescriptor_CodeValue,
					   CASE WHEN isperflevel = 1 THEN 'Level' ELSE 'Integer' end AS ResultDatatypeTypeDescriptor_Description,
	   
					   CASE WHEN isperflevel = 1 THEN 0 ELSE 1 end AS AssessmentScore_Indicator,
					   isperflevel AS AssessmentPerformanceLevel_Indicator,
  
					   0 AS ObjectiveAssessmentScore_Indicator,
					   0 AS ObjectiveAssessmentPerformanceLevel_Indicator,

					   '07/01/2015' AS AssessmentModifiedDate,
					   '07/01/2015' AS ValidFrom,
					   GETDATE() AS ValidTo,
					   0 AS IsCurrent,
					   1 AS IsLegacy
				FROM UnpivotedScores
			END

		TRUNCATE TABLE Staging.Assessment
		INSERT INTO Staging.Assessment
				   ([_sourceKey]
				   ,[AssessmentCategoryDescriptor_CodeValue]
				   ,[AssessmentCategoryDescriptor_Description]
				   ,[AssessmentFamilyTitle]
				   ,[AdaptiveAssessment_Indicator]
				   ,[AssessmentIdentifier]
				   ,[AssessmentTitle]

				   ,[ReportingMethodDescriptor_CodeValue]
				   ,[ReportingMethodDescriptor_Description]

				   ,[ResultDatatypeTypeDescriptor_CodeValue]
				   ,[ResultDatatypeTypeDescriptor_Description]

				   ,[AssessmentScore_Indicator]
				   ,[AssessmentPerformanceLevel_Indicator]

				   ,[ObjectiveAssessmentScore_Indicator]
				   ,[ObjectiveAssessmentPerformanceLevel_Indicator]
				   
				   ,AssessmentModifiedDate

				   ,[ValidFrom]
				   ,[ValidTo]
				   ,[IsCurrent])
        --declare @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate()
		SELECT DISTINCT 
				CONCAT_WS('|',CASE WHEN IsLegacy = 0 THEN 'Ed-Fi' ELSE 'LegacyDW' END , Convert(NVARCHAR(MAX),AssessmentIdentifier)  + '|' + Convert(NVARCHAR(MAX),ObjectiveAssessmentIdentificationCode) + '|' + Convert(NVARCHAR(MAX),ReportingMethodDescriptor_CodeValue)) AS [_sourceKey]
				
				,[AssessmentCategoryDescriptor_CodeValue]
			    ,[AssessmentCategoryDescriptor_Description]
			    ,[AssessmentFamilyTitle]
			    ,[AdaptiveAssessment_Indicator]
			    ,[AssessmentIdentifier]
			    ,[AssessmentTitle]
			    
			    ,[ReportingMethodDescriptor_CodeValue]
			    ,[ReportingMethodDescriptor_Description]
			    
			    ,[ResultDatatypeTypeDescriptor_CodeValue]
			    ,[ResultDatatypeTypeDescriptor_Description]
			    
			    ,[AssessmentScore_Indicator]
			    ,[AssessmentPerformanceLevel_Indicator]
			    
			    ,[ObjectiveAssessmentScore_Indicator]
			    ,[ObjectiveAssessmentPerformanceLevel_Indicator]
	  		    ,AssessmentModifiedDate
			    ,ValidFrom
			    ,ValidTo
			    ,IsCurrent
		FROM @Assessment;			    

				
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		
	END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimAssessment_PopulateProduction]
@LineageKey INT,
@LastDateLoaded DATETIME
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	    
		BEGIN TRANSACTION;   
		 

		--empty row technique
		--fact table should not have null foreign keys references
		--this empty record will be used in those cases
		IF NOT EXISTS (SELECT 1 
		               FROM dbo.DimAssessment WHERE _sourceKey = '')
				BEGIN
				   INSERT INTO dbo.DimAssessment
				   (
				       _sourceKey,
				       AssessmentCategoryDescriptor_CodeValue,
				       AssessmentCategoryDescriptor_Description,
				       AssessmentFamilyTitle,
				       AdaptiveAssessment_Indicator,
				       AssessmentIdentifier,
				       AssessmentTitle,
				       ReportingMethodDescriptor_CodeValue,
				       ReportingMethodDescriptor_Description,
				       ResultDatatypeTypeDescriptor_CodeValue,
				       ResultDatatypeTypeDescriptor_Description,
				       AssessmentScore_Indicator,
				       AssessmentPerformanceLevel_Indicator,
				       ObjectiveAssessmentScore_Indicator,
				       ObjectiveAssessmentPerformanceLevel_Indicator,
				       ValidFrom,
				       ValidTo,
				       IsCurrent,
				       LineageKey
				   )
				   VALUES
				   (   N'',       -- _sourceKey - nvarchar(2000)
				       N'N/A',       -- AssessmentCategoryDescriptor_CodeValue - nvarchar(50)
				       N'N/A',       -- AssessmentCategoryDescriptor_Description - nvarchar(1024)
				       N'N/A',       -- AssessmentFamilyTitle - nvarchar(100)
				       0,      -- AdaptiveAssessment_Indicator - bit
				       N'N/A',       -- AssessmentIdentifier - nvarchar(60)
				       N'N/A',       -- AssessmentTitle - nvarchar(500)
				       N'N/A',       -- ReportingMethodDescriptor_CodeValue - nvarchar(50)
				       N'N/A',       -- ReportingMethodDescriptor_Description - nvarchar(1024)
				       N'N/A',       -- ResultDatatypeTypeDescriptor_CodeValue - nvarchar(50)
				       N'N/A',       -- ResultDatatypeTypeDescriptor_Description - nvarchar(1024)
				       0,      -- AssessmentScore_Indicator - bit
				       0,      -- AssessmentPerformanceLevel_Indicator - bit
				       0,      -- ObjectiveAssessmentScore_Indicator - bit
				       0,      -- ObjectiveAssessmentPerformanceLevel_Indicator - bit
				       '07/01/2015', -- ValidFrom - datetime
					   '9999-12-31', -- ValidTo - datetime
					   0,      -- IsCurrent - bit
					   -1          -- LineageKey - int
				       )
				   
				  
				END

		--staging table holds newer records. 
		--the matching prod records will be valid until the date in which the newest data change was identified		
		UPDATE prod
		SET prod.ValidTo = stage.ValidFrom
		FROM 
			[dbo].DimAssessment AS prod
			INNER JOIN Staging.Assessment AS stage ON prod._sourceKey = stage._sourceKey
		WHERE prod.ValidTo = '12/31/9999'


		INSERT INTO dbo.DimAssessment
		(
		    _sourceKey,
		    AssessmentCategoryDescriptor_CodeValue,
		    AssessmentCategoryDescriptor_Description,
		    AssessmentFamilyTitle,
		    AdaptiveAssessment_Indicator,
		    AssessmentIdentifier,
		    AssessmentTitle,
		    ReportingMethodDescriptor_CodeValue,
		    ReportingMethodDescriptor_Description,
		    ResultDatatypeTypeDescriptor_CodeValue,
		    ResultDatatypeTypeDescriptor_Description,
		    AssessmentScore_Indicator,
		    AssessmentPerformanceLevel_Indicator,
		    ObjectiveAssessmentScore_Indicator,
		    ObjectiveAssessmentPerformanceLevel_Indicator,
		    ValidFrom,
		    ValidTo,
		    IsCurrent,
		    LineageKey
		)
		
		
		SELECT 
		    _sourceKey,
		    AssessmentCategoryDescriptor_CodeValue,
		    AssessmentCategoryDescriptor_Description,
		    AssessmentFamilyTitle,
		    AdaptiveAssessment_Indicator,
		    AssessmentIdentifier,
		    AssessmentTitle,
		    ReportingMethodDescriptor_CodeValue,
		    ReportingMethodDescriptor_Description,
		    ResultDatatypeTypeDescriptor_CodeValue,
		    ResultDatatypeTypeDescriptor_Description,
		    AssessmentScore_Indicator,
		    AssessmentPerformanceLevel_Indicator,
		    ObjectiveAssessmentScore_Indicator,
		    ObjectiveAssessmentPerformanceLevel_Indicator,
		    ValidFrom,
		    ValidTo,
		    IsCurrent,
		    @LineageKey
		FROM Staging.Assessment

		-- updating the EndTime to now and status to Success		
		UPDATE dbo.ETL_Lineage
			SET 
				EndTime = SYSDATETIME(),
				Status = 'S' -- success
		WHERE [LineageKey] = @LineageKey;
	
	
		-- Update the LoadDates table with the most current load date
		UPDATE [dbo].[ETL_IncrementalLoads]
		SET [LoadDate] = @LastDateLoaded
		WHERE [TableName] = N'dbo.DimAssessment';

		COMMIT TRANSACTION;		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		IF XACT_STATE( ) = -1
			BEGIN
				--The transaction is in an uncommittable state. Rolling back transaction
				ROLLBACK TRANSACTION;
			END;

		-- Test whether the transaction is committable.
		IF XACT_STATE( ) = 1
			BEGIN
				--The transaction is committable. Committing transaction
				COMMIT TRANSACTION;
			END;
	END CATCH;
END;
GO

--Dim Course
--------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimCourse_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY

		
		TRUNCATE TABLE Staging.Course
		INSERT INTO Staging.Course
		(
		    _sourceKey,
		    CourseCode,
		    CourseTitle,
		    CourseDescription,
		    CourseLevelCharacteristicTypeDescriptor_CodeValue,
		    CourseLevelCharacteristicTypeDescriptor_Description,
		    AcademicSubjectDescriptor_CodeValue,
		    AcademicSubjectDescriptor_Description,
		    HighSchoolCourseRequirement_Indicator,
		    MinimumAvailableCredits,
		    MaximumAvailableCredits,
		    GPAApplicabilityType_CodeValue,
		    GPAApplicabilityType_Description,
		    SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue,
		    SecondaryCourseLevelCharacteristicTypeDescriptor_Description,
		    CourseModifiedDate,
		    ValidFrom,
		    ValidTo,
		    IsCurrent
		)
		
        --declare @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate()
		SELECT DISTINCT 
			   CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),c.CourseCode)) AS [_sourceKey],
			   c.CourseCode,
			   c.CourseTitle,
			   c.CourseDescription,
			   COALESCE(clct.CodeValue,'N/A') AS [CourseLevelCharacteristicTypeDescriptor_CodeValue],
			   COALESCE(clct.[Description],'N/A') AS [CourseLevelCharacteristicTypeDescriptor_Descriptor],

			   COALESCE(ast.CodeValue,'N/A') AS [AcademicSubjectDescriptor_CodeValue],
			   COALESCE(ast.[Description],'N/A') AS [AcademicSubjectDescriptor_Descriptor],
			   COALESCE(c.HighSchoolCourseRequirement,0) AS [HighSchoolCourseRequirement_Indicator],

			   c.MinimumAvailableCredits,
			   c.MaximumAvailableCredits,
			   COALESCE(cgat.CodeValue,'N/A')  AS GPAApplicabilityType_CodeValue,
			   COALESCE(cgat.[Description],'N/A') AS GPAApplicabilityType_Description,
	   
			   'N/A' AS [SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue],
			   'N/A' AS [SecondaryCourseLevelCharacteristicTypeDescriptor_Description],
			   CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(c.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS CourseModifiedDate,

				--Making sure the first time, the ValidFrom is set to beginning of time 
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN
				           (SELECT MAX(t) FROM
                             (VALUES
                               (c.LastModifiedDate)                                               
                             ) AS [MaxLastModifiedDate](t)
                           )
					ELSE 
					      '07/01/2015' -- setting the validFrom to beggining of time during thre first load. 
				END AS ValidFrom,
			   '12/31/9999' as ValidTo,
				1 AS IsCurrent
		--select *
		FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Course c --WHERE c.CourseCode = '094'
			 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseLevelCharacteristic clc ON c.CourseCode = clc.CourseCode
			 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseLevelCharacteristicType clct ON clc.CourseLevelCharacteristicTypeId = clct.CourseLevelCharacteristicTypeId
			 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.AcademicSubjectType ast ON c.AcademicSubjectDescriptorId = ast.AcademicSubjectTypeId
			 LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseGPAApplicabilityType cgat ON c.CourseGPAApplicabilityTypeId = cgat.CourseGPAApplicabilityTypeId
		WHERE EXISTS (SELECT 1 
					  FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseOffering co 
					  WHERE c.CourseCode = co.CourseCode
						AND co.SchoolYear >=2019) AND
			 (c.LastModifiedDate > @LastLoadDate AND c.LastModifiedDate <= @NewLoadDate)
			
							
		
		--[v34_EdFi_BPS_Production_Ods]
		--loading legacy data if it has not been loaded.
		--load types are ignored as this data will only be loaded once.
		IF NOT EXISTS(SELECT 1 
		              FROM dbo.DimCourse 
					  WHERE CHARINDEX('LegacyDW',_sourceKey,1) > 0)
			BEGIN
			   INSERT INTO Staging.Course
				   (_sourceKey,
				    CourseCode,
					CourseTitle,
					CourseDescription,
					CourseLevelCharacteristicTypeDescriptor_CodeValue,
					CourseLevelCharacteristicTypeDescriptor_Description,
					AcademicSubjectDescriptor_CodeValue,
					AcademicSubjectDescriptor_Description,
					HighSchoolCourseRequirement_Indicator,
					MinimumAvailableCredits,
					MaximumAvailableCredits,
					GPAApplicabilityType_CodeValue,
					GPAApplicabilityType_Description,
					SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue,
					SecondaryCourseLevelCharacteristicTypeDescriptor_Description,
					CourseModifiedDate,
					ValidFrom,
					ValidTo,
					IsCurrent)
				 SELECT DISTINCT 
					   CONCAT('LegacyDW|',c.CourseNumber,'-',CASE WHEN s.SectionID = '' THEN 'N/A' ELSE s.SectionID END) AS [_sourceKey],
					   CONCAT_WS('-',c.CourseNumber,CASE WHEN s.SectionID = '' THEN 'N/A' ELSE s.SectionID END) AS [CourseCode],
					   c.TitleFull,
					   c.TitleFull,
					   COALESCE(c.LevelCode,'N/A') AS [CourseLevelCharacteristicTypeDescriptor_CodeValue],
					   COALESCE(c.LevelCode,'N/A') AS [CourseLevelCharacteristicTypeDescriptor_Description],

					   COALESCE(c.Department,'N/A') AS [AcademicSubjectDescriptor_CodeValue],
					   COALESCE(c.Department,'N/A') AS [AcademicSubjectDescriptor_Description],
					   0 AS [HighSchoolCourseRequirement_Indicator],

					   NULL AS MinimumAvailableCredits,
					   NULL AS MaximumAvailableCredits,
					   CASE WHEN COALESCE(c.InGPA,'N') = 'Y' THEN 'Applicable' ELSE 'Not Applicable' END  AS GPAApplicabilityType_CodeValue,
					   CASE WHEN COALESCE(c.InGPA,'N') = 'Y' THEN 'Applicable' ELSE 'Not Applicable' END AS GPAApplicabilityType_Description,
					   COALESCE(c.MassCore,'N/A') AS [SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue],
					   COALESCE(c.MassCore,'N/A') AS [SecondaryCourseLevelCharacteristicTypeDescriptor_Description],
					   '07/01/2015' AS CourseModifiedDate,
					   '07/01/2015' AS ValidFrom,
					    GETDATE() as ValidTo,
						0 AS IsCurrent
				--select *
				FROM [BPSGranary02].[RAEDatabase].[dbo].[CourseCatalog_aspen] c  --   WHERE CourseNumber = '094' AND SchoolYear IN ('2017-2018','2016-2017','2015-2016')
					 INNER JOIN [BPSGranary02].[RAEDatabase].[dbo].[StudentCourseGrade_aspenNewFormat] s ON c.CourseNumber = s.CourseNumber
																										AND c.SchoolYear = s.SchoolYear
				WHERE c.SchoolYear IN ('2017-2018','2016-2017','2015-2016');
			

			END

		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		
	END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimCourse_PopulateProduction]
@LineageKey INT,
@LastDateLoaded DATETIME
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	    
		BEGIN TRANSACTION;   
		 
		--empty row technique
		--fact table should not have null foreign keys references
		--this empty record will be used in those cases
		IF NOT EXISTS (SELECT 1 
		               FROM dbo.DimCourse WHERE _sourceKey = '')
				BEGIN
				   INSERT INTO dbo.DimCourse
				   (
				       _sourceKey,
				       CourseCode,
				       CourseTitle,
				       CourseDescription,
				       CourseLevelCharacteristicTypeDescriptor_CodeValue,
				       CourseLevelCharacteristicTypeDescriptor_Description,
				       AcademicSubjectDescriptor_CodeValue,
				       AcademicSubjectDescriptor_Description,
				       HighSchoolCourseRequirement_Indicator,
				       MinimumAvailableCredits,
				       MaximumAvailableCredits,
				       GPAApplicabilityType_CodeValue,
				       GPAApplicabilityType_Description,
				       SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue,
				       SecondaryCourseLevelCharacteristicTypeDescriptor_Description,
				       ValidFrom,
				       ValidTo,
				       IsCurrent,
				       LineageKey
				   )
				   VALUES
				   (   N'',       -- _sourceKey - nvarchar(50)
				       N'N/A',       -- CourseCode - nvarchar(60)
				       N'N/A',       -- CourseTitle - nvarchar(100)
				       N'N/A',       -- CourseDescription - nvarchar(100)
				       N'N/A',       -- CourseLevelCharacteristicTypeDescriptor_CodeValue - nvarchar(60)
				       N'N/A',       -- CourseLevelCharacteristicTypeDescriptor_Description - nvarchar(1024)
				       N'N/A',       -- AcademicSubjectDescriptor_CodeValue - nvarchar(60)
				       N'N/A',       -- AcademicSubjectDescriptor_Description - nvarchar(1024)
				       0,      -- HighSchoolCourseRequirement_Indicator - bit
				       0,         -- MinimumAvailableCredits - int
				       0,         -- MaximumAvailableCredits - int
				       N'N/A',       -- GPAApplicabilityType_CodeValue - nvarchar(50)
				       N'N/A',       -- GPAApplicabilityType_Description - nvarchar(50)
				       N'N/A',       -- SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue - nvarchar(50)
				       N'N/A',       -- SecondaryCourseLevelCharacteristicTypeDescriptor_Description - nvarchar(50)
				      '07/01/2015', -- ValidFrom - datetime
					   '9999-12-31', -- ValidTo - datetime
					   0,      -- IsCurrent - bit
					   -1          -- LineageKey - int
				       )
				  
				   
				  
				END

		--staging table holds newer records. 
		--the matching prod records will be valid until the date in which the newest data change was identified		
		UPDATE prod
		SET prod.ValidTo = stage.ValidFrom
		FROM 
			[dbo].[DimCourse] AS prod
			INNER JOIN Staging.Course AS stage ON prod._sourceKey = stage._sourceKey
		WHERE prod.ValidTo = '12/31/9999'


		INSERT INTO dbo.DimCourse
		(
		    _sourceKey,
			CourseCode,
			CourseTitle,
			CourseDescription,
			CourseLevelCharacteristicTypeDescriptor_CodeValue,
			CourseLevelCharacteristicTypeDescriptor_Description,
			AcademicSubjectDescriptor_CodeValue,
			AcademicSubjectDescriptor_Description,
			HighSchoolCourseRequirement_Indicator,
			MinimumAvailableCredits,
			MaximumAvailableCredits,
			GPAApplicabilityType_CodeValue,
			GPAApplicabilityType_Description,
			SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue,
			SecondaryCourseLevelCharacteristicTypeDescriptor_Description,			
			ValidFrom,
			ValidTo,
			IsCurrent,
			LineageKey
		)
		
		SELECT 
		    _sourceKey,
			CourseCode,
			CourseTitle,
			CourseDescription,
			CourseLevelCharacteristicTypeDescriptor_CodeValue,
			CourseLevelCharacteristicTypeDescriptor_Description,
			AcademicSubjectDescriptor_CodeValue,
			AcademicSubjectDescriptor_Description,
			HighSchoolCourseRequirement_Indicator,
			MinimumAvailableCredits,
			MaximumAvailableCredits,
			GPAApplicabilityType_CodeValue,
			GPAApplicabilityType_Description,
			SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue,
			SecondaryCourseLevelCharacteristicTypeDescriptor_Description,			
			ValidFrom,
			ValidTo,
			IsCurrent,
		    @LineageKey
		FROM Staging.Course

		-- updating the EndTime to now and status to Success		
		UPDATE dbo.ETL_Lineage
			SET 
				EndTime = SYSDATETIME(),
				Status = 'S' -- success
		WHERE [LineageKey] = @LineageKey;
	
	
		-- Update the LoadDates table with the most current load date
		UPDATE [dbo].[ETL_IncrementalLoads]
		SET [LoadDate] = @LastDateLoaded
		WHERE [TableName] = N'dbo.DimCourse';

		COMMIT TRANSACTION;		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		IF XACT_STATE( ) = -1
			BEGIN
				--The transaction is in an uncommittable state. Rolling back transaction
				ROLLBACK TRANSACTION;
			END;

		-- Test whether the transaction is committable.
		IF XACT_STATE( ) = 1
			BEGIN
				--The transaction is committable. Committing transaction
				COMMIT TRANSACTION;
			END;
	END CATCH;
END;
GO


--Fact StudentAttendanceByDay
----------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_FactStudentAttendanceByDay_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	
		--DECLARE @LastLoadDate datetime= '07/01/2015' DECLARE @NewLoadDate datetime = GETDATE()
		TRUNCATE TABLE Staging.StudentAttendanceByDay	
		CREATE TABLE #StudentsToBeProcessed (StudentUSI INT, 
		                                     EventDate DATE ,
											 LastModifiedDate DATETIME )
		  
		CREATE TABLE #AttedanceEventRankedByReason (StudentUSI INT, 
		                                            SchoolId INT, 
													SchoolYear SMALLINT, 
													EventDate DATE, 
													LastModifiedDate DATETIME,
													AttendanceEventCategoryDescriptorId INT,
													AttendanceEventReason NVARCHAR(max) , 
													RowId INT  )
	    CREATE TABLE #DistinctAttedanceEvents (StudentUSI INT, 
		                                       SchoolId INT, 
											   SchoolYear SMALLINT, 
											   EventDate DATE, 
											   LastModifiedDate DATETIME,
											   AttendanceEventCategoryDescriptorId INT,
											   AttendanceEventReason NVARCHAR(max))
	
		CREATE NONCLUSTERED INDEX [#AttedanceEventRankedByReason_MainCovering]
		ON [dbo].[#AttedanceEventRankedByReason] ([StudentUSI],[SchoolId],[EventDate],[RowId])
		INCLUDE ([AttendanceEventCategoryDescriptorId],[AttendanceEventReason])


		INSERT INTO #DistinctAttedanceEvents
		(
		    StudentUSI,
		    SchoolId,
		    SchoolYear,
		    EventDate,
			LastModifiedDate,
		    AttendanceEventCategoryDescriptorId,
		    AttendanceEventReason
		)
		SELECT   DISTINCT 
					StudentUSI, 
					SchoolId, 
					SchoolYear, 
					EventDate,
					LastModifiedDate,
					AttendanceEventCategoryDescriptorId,					
					LTRIM(RTRIM(COALESCE(AttendanceEventReason,''))) AS AttendanceEventReason 
		FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAttendanceEvent
		WHERE SchoolYear >= 2019
			AND (LastModifiedDate > @LastLoadDate  AND LastModifiedDate <= @NewLoadDate)
		

		INSERT INTO #AttedanceEventRankedByReason
		(
			StudentUSI,
			SchoolId,
			SchoolYear,
			EventDate,
			LastModifiedDate,
			AttendanceEventCategoryDescriptorId,
			AttendanceEventReason,
			RowId
		)
		SELECT DISTINCT  
		            StudentUSI, 
					SchoolId, 
					SchoolYear, 
					EventDate,
					LastModifiedDate,
					AttendanceEventCategoryDescriptorId,
					AttendanceEventReason , 
					ROW_NUMBER() OVER (PARTITION BY StudentUSI, 
													SchoolId, 
													SchoolYear, 
													EventDate,
													AttendanceEventCategoryDescriptorId
										ORDER BY AttendanceEventReason DESC) AS RowId 
			FROM #DistinctAttedanceEvents

			
		IF (@LastLoadDate <> '07/01/2015')
			BEGIN
				INSERT INTO #StudentsToBeProcessed (StudentUSI, EventDate, LastModifiedDate)
				SELECT DISTINCT StudentUSI, EventDate, LastModifiedDate
				FROM #DistinctAttedanceEvents
			END
	    ELSE --this first time all students will be processed
			BEGIN
				INSERT INTO #StudentsToBeProcessed (StudentUSI, EventDate, LastModifiedDate)
				SELECT DISTINCT StudentUSI, NULL AS EventDate, NULL AS LastModifiedDate --we don't care about event changes the first this runs. 
				FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation
				WHERE SchoolYear >= 2019
			END;
		
		
		
		INSERT INTO Staging.StudentAttendanceByDay
		(
		    _sourceKey,
		    StudentKey,
		    TimeKey,
		    SchoolKey,
		    AttendanceEventCategoryKey,
		    AttendanceEventReason,
		    ModifiedDate,
		    _sourceStudentKey,
		    _sourceTimeKey,
		    _sourceSchoolKey,
		    _sourceAttendanceEventCategoryKey
		)	
		SELECT DISTINCT         
				  CONCAT_WS('|',Convert(NVARCHAR(MAX),ssa.StudentUSI),CONVERT(CHAR(10), cdce.Date, 101)) AS _sourceKey,
				  NULL AS StudentKey,
				  NULL AS TimeKey,	  
				  NULL AS SchoolKey,  
				  NULL AS AttendanceEventCategoryKey,				  
				  ISNULL(ssae.AttendanceEventReason,'') AS AttendanceEventReason,
				  --stbp.LastModifiedDate only makes sense when identifying deltas, the first time we just follow the calendar date
				  cdce.Date AS ModifiedDate,
				  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),ssa.StudentUSI)) AS _sourceStudentKey,
		          cdce.Date AS _sourceTimeKey,		          
				  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),ssa.SchoolId))  AS _sourceSchoolKey,
		          CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),ssae.AttendanceEventCategoryDescriptorId))  AS _sourceAttendanceEventCategoryKey
				  				  
			--select *  
			FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
				INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CalendarDate cda on ssa.SchoolId = cda.SchoolId 														   
				INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CalendarDateCalendarEvent cdce on cda.Date=cdce.Date 
																					 and cda.SchoolId=cdce.SchoolId
				INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d_cdce on cdce.CalendarEventDescriptorId = d_cdce.DescriptorId
																	  and d_cdce.CodeValue='Instructional day' -- ONLY Instructional days
	            INNER JOIN  #StudentsToBeProcessed stbp ON ssa.StudentUSI = stbp.StudentUSI
				                                      AND (stbp.EventDate IS NULL OR 
													       cdce.Date = stbp.EventDate)
				LEFT JOIN #AttedanceEventRankedByReason ssae on ssa.StudentUSI = ssae.StudentUSI
															   AND ssa.SchoolId = ssae.SchoolId 
															   AND cda.Date = ssae.EventDate
															   AND ssae.RowId= 1			
			WHERE  cdce.Date >= ssa.EntryDate 
			   AND cdce.Date <= GETDATE()
			   AND (
					 (ssa.ExitWithdrawDate is null) 
					  OR
					 (ssa.ExitWithdrawDate is not null and cdce.Date<=ssa.ExitWithdrawDate) 
				   )
				AND ssa.SchoolYear >= 2019
				
			DROP TABLE #StudentsToBeProcessed, #AttedanceEventRankedByReason, #DistinctAttedanceEvents;
			
			
		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		
	END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_FactStudentAttendanceByDay_PopulateProduction]
@LineageKey INT,
@LastDateLoaded DATETIME
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	    
		BEGIN TRANSACTION;   
		 
		
      
	    --updating staging keys
		UPDATE s 
		SET s.StudentKey = (
								SELECT TOP (1) ds.StudentKey
								FROM dbo.DimStudent ds
								WHERE s._sourceStudentKey = ds._sourceKey									
									AND s.[ModifiedDate] >= ds.[ValidFrom]
									AND s.[ModifiedDate] < ds.[ValidTo]
								ORDER BY ds.[ValidFrom] DESC
							),
			s.TimeKey = (
							SELECT TOP (1) dt.TimeKey
							FROM dbo.DimTime dt
									INNER JOIN dbo.DimSchool ds ON dt.SchoolKey = ds.SchoolKey
							WHERE s._sourceSchoolKey = ds._sourceKey
								AND s._sourceTimeKey = dt.SchoolDate
							ORDER BY dt.SchoolDate
						),
			s.SchoolKey = (
								SELECT TOP (1) ds.SchoolKey
								FROM dbo.DimSchool ds
								WHERE s._sourceSchoolKey = ds._sourceKey									
									AND s.[ModifiedDate] >= ds.[ValidFrom]
									AND s.[ModifiedDate] < ds.[ValidTo]
								ORDER BY ds.[ValidFrom] DESC
							),		
			s.AttendanceEventCategoryKey = COALESCE(
													(
														SELECT TOP (1) daec.AttendanceEventCategoryKey
														FROM dbo.DimAttendanceEventCategory daec
														WHERE s._sourceAttendanceEventCategoryKey = daec._sourceKey									
														  AND s.[ModifiedDate] >= daec.[ValidFrom]
														  AND s.[ModifiedDate] < daec.[ValidTo]
														ORDER BY daec.[ValidFrom]
													), 
													(
													 SELECT TOP(1) AttendanceEventCategoryKey 
													 FROM [dbo].DimAttendanceEventCategory 
													 WHERE AttendanceEventCategoryDescriptor_CodeValue = 'In Attendance'
													)  
										          )     
        FROM Staging.StudentAttendanceByDay s;
		
		DELETE FROM Staging.StudentAttendanceByDay
		WHERE StudentKey IS NULL OR 
		      TimeKey IS NULL OR
			  SchoolKey IS NULL OR
			  AttendanceEventCategoryKey IS NULL;

		--dropping the columnstore index
		DROP INDEX IF EXISTS CSI_FactStudentAttendanceByDay ON dbo.FactStudentAttendanceByDay;

		--deleting changed records
		DELETE prod
		FROM [dbo].FactStudentAttendanceByDay AS prod
		WHERE EXISTS (SELECT 1 
		              FROM [Staging].StudentAttendanceByDay stage
					  WHERE prod._sourceKey = stage._sourceKey)
					  
	    
		
		INSERT INTO dbo.FactStudentAttendanceByDay
		(
		    _sourceKey,
		    StudentKey,
		    TimeKey,
		    SchoolKey,
		    AttendanceEventCategoryKey,
		    AttendanceEventReason,
		    LineageKey
		)
		SELECT DISTINCT 
		    _sourceKey,
		    StudentKey,
		    TimeKey,
		    SchoolKey,
		    AttendanceEventCategoryKey,
		    AttendanceEventReason,
			@LineageKey		
		FROM Staging.StudentAttendanceByDay

			
		--loading from legacy dw just once
		IF (NOT EXISTS(SELECT 1  
		               FROM dbo.FactStudentAttendanceByDay 
		               WHERE _sourceKey = 'LegacyDW'))
			  BEGIN
			     INSERT INTO EdFiDW.[dbo].[FactStudentAttendanceByDay]
					   (_sourceKey,
					    [StudentKey]
					   ,[TimeKey]
					   ,[SchoolKey]
					   ,[AttendanceEventCategoryKey]
					   ,[AttendanceEventReason]
					   ,[LineageKey])
				SELECT 
				      'LegacyDW' AS _sourceKey,
					  ds.StudentKey,
					  dt.TimeKey,	  
					  dschool.SchoolKey,      
					  daec.AttendanceEventCategoryKey,
					  'N/A' AS  AttendanceEventReason,
					  @LineageKey AS [LineageKey]
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
 
				WHERE  a.[Date] BETWEEN '2015-07-01' AND '2018-06-30'
				   AND a.sch between '1000' and '4700'
			  END

		--re-creating the columnstore index
		CREATE COLUMNSTORE INDEX CSI_FactStudentAttendanceByDay
			  ON dbo.FactStudentAttendanceByDay
			  ([StudentKey]
			  ,[TimeKey]
			  ,[SchoolKey]
			  ,[AttendanceEventCategoryKey]
			  ,[AttendanceEventReason]
			  ,[LineageKey])

		--Deriving
		--dropping the columnstore index
		DROP INDEX IF EXISTS CSI_Derived_StudentAttendanceByDay ON Derived.StudentAttendanceByDay;

		--ByDay
		delete d_sabd
		FROM  [Derived].[StudentAttendanceByDay] d_sabd
		WHERE EXISTS(SELECT 1 
		             FROM Staging.StudentAttendanceByDay s_sabd
					 WHERE d_sabd.StudentKey = s_sabd.StudentKey
					    AND d_sabd.[TimeKey] = s_sabd.[TimeKey])

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
				        INNER JOIN Staging.StudentAttendanceByDay s_sabd ON fsabd.StudentKey = s_sabd.StudentKey
					                                                    AND fsabd.[TimeKey] = s_sabd.[TimeKey]
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
			
		--ADA
		
		DELETE d_sabd
		FROM  [Derived].[StudentAttendanceADA] d_sabd
		WHERE EXISTS(SELECT 1 
		             FROM Staging.StudentAttendanceByDay s_sabd
					      INNER JOIN dbo.DimTime dt ON s_sabd.TimeKey = dt.TimeKey
						  INNER JOIN dbo.DimStudent st ON s_sabd.StudentKey = st.StudentKey
					 WHERE d_sabd.StudentId = st.StudentUniqueId
					   AND d_sabd.[SchoolYear] = dt.SchoolYear)
					   
		INSERT INTO [Derived].[StudentAttendanceADA]([StudentId]
																
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

		SELECT     DISTINCT
					v_sabd.StudentId, 
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
		FROM dbo.View_StudentAttendanceByDay v_sabd		
		WHERE EXISTS(SELECT 1 
		             FROM Staging.StudentAttendanceByDay s_sabd
					      INNER JOIN dbo.DimTime dt ON s_sabd.TimeKey = dt.TimeKey
						  INNER JOIN dbo.DimStudent st ON s_sabd.StudentKey = st.StudentKey
					 WHERE v_sabd.StudentId = st.StudentUniqueId
					   AND v_sabd.[SchoolYear] = dt.SchoolYear)
		GROUP BY    v_sabd.StudentId, 
					
					v_sabd.FirstName, 
					v_sabd.LastName, 
					v_sabd.[DistrictSchoolCode],
					v_sabd.[UmbrellaSchoolCode],	   
					v_sabd.SchoolName, 	   
					v_sabd.SchoolYear

		

        -- updating the EndTime to now and status to Success		
		UPDATE dbo.ETL_Lineage
			SET 
				EndTime = SYSDATETIME(),
				Status = 'S' -- success
		WHERE [LineageKey] = @LineageKey;
	
	
		-- Update the LoadDates table with the most current load date
		UPDATE [dbo].[ETL_IncrementalLoads]
		SET [LoadDate] = @LastDateLoaded
		WHERE [TableName] = N'dbo.FactStudentAttendanceByDay';
		
	    
		COMMIT TRANSACTION;		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		IF XACT_STATE( ) = -1
			BEGIN
				--The transaction is in an uncommittable state. Rolling back transaction
				ROLLBACK TRANSACTION;
			END;

		-- Test whether the transaction is committable.
		IF XACT_STATE( ) = 1
			BEGIN
				--The transaction is committable. Committing transaction
				COMMIT TRANSACTION;
			END;
	END CATCH;
END;
GO


--Fact StudentDiscipline
----------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_FactStudentDiscipline_PopulateStaging] 
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
    
		--DECLARE @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate();
		TRUNCATE TABLE Staging.StudentDiscipline
			
		INSERT INTO Staging.StudentDiscipline
		(
		    _sourceKey,
		    StudentKey,
		    TimeKey,
		    SchoolKey,
		    DisciplineIncidentKey,
		    ModifiedDate,
		    _sourceStudentKey,
		    _sourceTimeKey,
		    _sourceSchoolKey,
		    _sourceDisciplineIncidentKey
		)
		
		SELECT DISTINCT 
		       CONCAT_WS('|',Convert(NVARCHAR(MAX),sdia.StudentUSI),di.IncidentIdentifier) AS _sourceKey,
			   NULL AS StudentKey,
			   NULL AS TimeKey,	  
			   NULL AS SchoolKey,  
			   NULL AS DisciplineIncidentKey,  
			   di.IncidentDate AS ModifiedDate,
			   CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),sdia.StudentUSI)) AS _sourceStudentKey,
		       di.IncidentDate AS _sourceTimeKey,		          
			   CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),di.SchoolId))  AS _sourceSchoolKey,
		       CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),di.IncidentIdentifier))  AS  _sourceDisciplineIncidentKey

		FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.DisciplineIncident di       
			  INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentDisciplineIncidentAssociation sdia ON di.IncidentIdentifier = sdia.IncidentIdentifier
		WHERE di.IncidentDate >= '07/01/2018'	  
		 AND  (
		       (di.LastModifiedDate > @LastLoadDate  AND di.LastModifiedDate <= @NewLoadDate)
			     OR
		       (sdia.LastModifiedDate > @LastLoadDate  AND sdia.LastModifiedDate <= @NewLoadDate)
			  )
		 
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		
	END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_FactStudentDiscipline_PopulateProduction]
@LineageKey INT,
@LastDateLoaded DATETIME
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	    
		BEGIN TRANSACTION;   
		 
		--updating staging keys
		UPDATE s 
		SET s.StudentKey = (
								SELECT TOP (1) ds.StudentKey
								FROM dbo.DimStudent ds
								WHERE s._sourceStudentKey = ds._sourceKey									
									AND s.[ModifiedDate] >= ds.[ValidFrom]
									AND s.[ModifiedDate] < ds.[ValidTo]
								ORDER BY ds.[ValidFrom] DESC
							),
			s.TimeKey = (
							SELECT TOP (1) dt.TimeKey
							FROM dbo.DimTime dt
									INNER JOIN dbo.DimSchool ds ON dt.SchoolKey = ds.SchoolKey
							WHERE s._sourceSchoolKey = ds._sourceKey
								AND s._sourceTimeKey = dt.SchoolDate
							ORDER BY dt.SchoolDate
						),
			s.SchoolKey = (
								SELECT TOP (1) ds.SchoolKey
								FROM dbo.DimSchool ds
								WHERE s._sourceSchoolKey = ds._sourceKey									
									AND s.[ModifiedDate] >= ds.[ValidFrom]
									AND s.[ModifiedDate] < ds.[ValidTo]
								ORDER BY ds.[ValidFrom] DESC
							),		
			s.DisciplineIncidentKey =(
										SELECT TOP (1) ddi.DisciplineIncidentKey
										FROM dbo.DimDisciplineIncident ddi
										WHERE s._sourceDisciplineIncidentKey = ddi._sourceKey									
											AND s.[ModifiedDate] >= ddi.[ValidFrom]
											AND s.[ModifiedDate] < ddi.[ValidTo]
										ORDER BY ddi.[ValidFrom] DESC
									)  
										             
        FROM Staging.StudentDiscipline s;
		
		DELETE FROM Staging.StudentDiscipline
		WHERE StudentKey IS NULL OR 
		      TimeKey IS NULL OR
			  SchoolKey IS NULL OR
			  DisciplineIncidentKey IS NULL;

		--dropping the columnstore index
        DROP INDEX IF EXISTS CSI_FactStudentDiscipline ON dbo.FactStudentDiscipline;
	    
		--deleting changed records
		DELETE prod
		FROM [dbo].[FactStudentDiscipline] AS prod
		WHERE EXISTS (SELECT 1 
		              FROM [Staging].StudentDiscipline stage
					  WHERE prod._sourceKey = stage._sourceKey)



		INSERT INTO [dbo].[FactStudentDiscipline]
		   		([_sourceKey]
				,[StudentKey]
		   		,[TimeKey]
		   		,[SchoolKey]
		   		,[DisciplineIncidentKey]           
		   		,[LineageKey])
		SELECT DISTINCT 
		    _sourceKey,
		    StudentKey,
		   	TimeKey,
		   	SchoolKey,	   
		   	DisciplineIncidentKey,	   
		   	@lineageKey AS LineageKey
		FROM Staging.StudentDiscipline


		--loading from legacy dw just once
		IF (NOT EXISTS(SELECT 1  
		               FROM dbo.FactStudentDiscipline 
		               WHERE _sourceKey = 'LegacyDW'))
             BEGIN					
					INSERT INTO [dbo].[FactStudentDiscipline]
							   ([_sourceKey]
								,[StudentKey]
							   ,[TimeKey]
							   ,[SchoolKey]
							   ,[DisciplineIncidentKey]           
							   ,[LineageKey])

					SELECT DISTINCT 
						   'LegacyDW',
						   ds.StudentKey,
						   dt.TimeKey,
						   dschool.SchoolKey,	   
						   d_di.[DisciplineIncidentKey],	   
						   @lineageKey AS LineageKey
					FROM  [Raw_LegacyDW].[DisciplineIncidents] di    
						  INNER JOIN dbo.DimStudent ds  ON CONCAT_WS('|', 'LegacyDW', Convert(NVARCHAR(MAX),di.BPS_Student_ID))   = ds._sourceKey
															 AND	 di.CND_INCIDENT_DATE BETWEEN ds.ValidFrom AND ds.ValidTo
						  INNER JOIN dbo.DimSchool dschool ON CONCAT_WS('|', 'Ed-Fi', Convert(NVARCHAR(MAX),di.[SKL_SCHOOL_ID]))   = dschool._sourceKey 
															 AND	 di.CND_INCIDENT_DATE BETWEEN dschool.ValidFrom AND dschool.ValidTo
						  INNER JOIN dbo.DimTime dt ON di.CND_INCIDENT_DATE = dt.SchoolDate
														  AND dt.SchoolKey is not null   
														  AND dschool.SchoolKey = dt.SchoolKey
						  INNER JOIN dbo.DimDisciplineIncident d_di ON CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),di.CND_INCIDENT_ID))    = d_di._sourceKey
					WHERE TRY_CAST(di.CND_INCIDENT_DATE AS DATETIME)  BETWEEN '2015-09-01' AND '2018-06-30' 
			 END;

        --re-creating the columnstore index
		CREATE COLUMNSTORE INDEX CSI_FactStudentDiscipline
				ON dbo.FactStudentDiscipline
				([StudentKey]
				,[TimeKey]
				,[SchoolKey]
				,[DisciplineIncidentKey]
				,[LineageKey])
				

		-- updating the EndTime to now and status to Success		
		UPDATE dbo.ETL_Lineage
			SET 
				EndTime = SYSDATETIME(),
				Status = 'S' -- success
		WHERE [LineageKey] = @LineageKey;
	
	
		-- Update the LoadDates table with the most current load date
		UPDATE [dbo].[ETL_IncrementalLoads]
		SET [LoadDate] = @LastDateLoaded
		WHERE [TableName] = N'dbo.FactStudentDiscipline';

		
	    
		COMMIT TRANSACTION;		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		IF XACT_STATE( ) = -1
			BEGIN
				--The transaction is in an uncommittable state. Rolling back transaction
				ROLLBACK TRANSACTION;
			END;

		-- Test whether the transaction is committable.
		IF XACT_STATE( ) = 1
			BEGIN
				--The transaction is committable. Committing transaction
				COMMIT TRANSACTION;
			END;
	END CATCH;
END;
GO

--Fact StudentAssessmentScore
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_FactStudentAssessmentScore_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
    
		--DECLARE @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate();
		--select * from Staging.StudentAssessmentScore
		TRUNCATE TABLE Staging.StudentAssessmentScore
			
		INSERT INTO Staging.StudentAssessmentScore
		(
		    _sourceKey,
		    StudentKey,
		    TimeKey,
		    AssessmentKey,
		    ScoreResult,
		    IntegerScoreResult,
		    DecimalScoreResult,
		    LiteralScoreResult,
		    ModifiedDate,
		    _sourceStudentKey,
		    _sourceTimeKey,
		    _sourceAssessmentKey
		)
		
		
		SELECT   DISTINCT 
			      CONCAT_WS('|',CONVERT(NVARCHAR(MAX),s.StudentUSI),sa.StudentAssessmentIdentifier) AS _sourceKey,				  
				  NULL AS StudentKey,
				  NULL AS TimeKey,	  
				  NULL AS AssessmentKey,
				  sas.Result AS [SoreResult],
				  CASE when ascr_rdtt.CodeValue in ('Integer') AND TRY_CAST(sas.Result AS INTEGER) IS NOT NULL AND sas.Result <> '-' THEN sas.Result ELSE NULL END AS IntegerScoreResult,
				  CASE when ascr_rdtt.CodeValue in ('Decimal','Percentage','Percentile')  AND TRY_CAST(sas.Result AS FLOAT)  IS NOT NULL THEN sas.Result ELSE NULL END AS DecimalScoreResult,
				  CASE when ascr_rdtt.CodeValue not in ('Integer','Decimal','Percentage','Percentile') THEN sas.Result ELSE NULL END AS LiteralScoreResult,
				  CONVERT(DATE ,sa.AdministrationDate) AS ModifiedDate ,
				  CONCAT_WS('|','Ed-Fi',CONVERT(NVARCHAR(MAX),s.StudentUSI)) AS  _sourceStudentKey,
				  CONVERT(DATE ,sa.AdministrationDate) AS  _sourceTimeKey,
				  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),sa.AssessmentIdentifier),'N/A',Convert(NVARCHAR(MAX),armt.CodeValue)) AS  _sourceAssessmentKey
			--select top 1 'Ed-Fi|' + Convert(NVARCHAR(MAX),sa.AssessmentIdentifier)  + '|N/A|' + Convert(NVARCHAR(MAX),armt.CodeValue), sa.AdministrationDate,*  
			FROM [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].Student s 
      
				--student assessment
				inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].StudentAssessment sa on sa.StudentUSI = s.StudentUSI
      
				--student assessment score results
				inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].StudentAssessmentScoreResult sas on sa.StudentAssessmentIdentifier = sas.StudentAssessmentIdentifier
																and sa.AssessmentIdentifier = sas.AssessmentIdentifier

				--assessment 
				inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].Assessment a on sa.AssessmentIdentifier = a.AssessmentIdentifier 

				inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].AssessmentScore ascr on sas.AssessmentIdentifier = ascr.AssessmentIdentifier 
													and sas.[AssessmentReportingMethodTypeId] = ascr.[AssessmentReportingMethodTypeId]
				inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].[AssessmentReportingMethodType] armt on ascr.[AssessmentReportingMethodTypeId] = armt.[AssessmentReportingMethodTypeId]

				INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ResultDatatypeType ascr_rdtt ON ascr.ResultDatatypeTypeId = ascr_rdtt.ResultDatatypeTypeId	
				
			WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1 
				 AND sa.AdministrationDate >= '07/01/2018'		
				 AND  (
					   (sa.LastModifiedDate > @LastLoadDate  AND sa.LastModifiedDate <= @NewLoadDate)			     
					  )

		UNION ALL
		SELECT   DISTINCT 
			      CONCAT_WS('|',CONVERT(NVARCHAR(MAX),s.StudentUSI),sa.StudentAssessmentIdentifier),
				  NULL AS StudentKey,
				  NULL AS TimeKey,	  
				  NULL AS AssessmentKey,
				  apl_ld.CodeValue AS [SoreResult],
				  NULL AS IntegerScoreResult,
				  NULL AS DecimalScoreResult,
				  apl_ld.CodeValue AS LiteralScoreResult,	  
				  CONVERT(DATE ,sa.AdministrationDate) AS ModifiedDate ,
				  CONCAT_WS('|','Ed-Fi',CONVERT(NVARCHAR(MAX),s.StudentUSI)) AS  _sourceStudentKey,
				  CONVERT(DATE ,sa.AdministrationDate) AS  _sourceTimeKey,
				  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),sa.AssessmentIdentifier),'N/A',Convert(NVARCHAR(MAX),apl_sd.CodeValue)) AS  _sourceAssessmentKey
			--select top 100 *  
			FROM [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].Student s 
      
				--student assessment
				inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].StudentAssessment sa on sa.StudentUSI = s.StudentUSI 
	
				inner  join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].StudentAssessmentPerformanceLevel sapl on sa.StudentAssessmentIdentifier = sapl.StudentAssessmentIdentifier
																		 and sa.AssessmentIdentifier = sapl.AssessmentIdentifier
															 --    and apl.PerformanceLevelDescriptorId = sapl.PerformanceLevelDescriptorId
    
				--assessment 
				inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].Assessment a on sa.AssessmentIdentifier = a.AssessmentIdentifier 

				inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].[AssessmentPerformanceLevel] apl on sa.AssessmentIdentifier = apl.AssessmentIdentifier 
																 and sapl.[AssessmentReportingMethodTypeId] = apl.[AssessmentReportingMethodTypeId]
																 and sapl.PerformanceLevelDescriptorId = apl.PerformanceLevelDescriptorId
    
				inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].[AssessmentReportingMethodType] apl_sd on apl.[AssessmentReportingMethodTypeId] = apl_sd.[AssessmentReportingMethodTypeId] 
				inner join [EDFISQL01].[EdFi_BPS_Production_Ods].[edfi].Descriptor apl_ld on apl.PerformanceLevelDescriptorId = apl_ld.DescriptorId 

			WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1           
				 AND sa.AdministrationDate >= '07/18/2018'
				 AND  (
					   (sa.LastModifiedDate > @LastLoadDate  AND sa.LastModifiedDate <= @NewLoadDate)			     
					  )
		 
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		
	END CATCH;
END;
GO
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_FactStudentAssessmentScore_PopulateProduction]
@LineageKey INT,
@LastDateLoaded DATETIME
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	    
		BEGIN TRANSACTION;   
		 
		--updating staging keys
		UPDATE s 
		SET s.StudentKey = (
								SELECT TOP (1) ds.StudentKey
								FROM dbo.DimStudent ds
								WHERE s._sourceStudentKey = ds._sourceKey									
									AND s.[ModifiedDate] >= ds.[ValidFrom]
									AND s.[ModifiedDate] < ds.[ValidTo]
								ORDER BY ds.[ValidFrom] DESC
							),
			s.TimeKey = (
							SELECT TOP (1) dt.TimeKey
							FROM dbo.DimTime dt									
							WHERE s._sourceTimeKey = dt.SchoolDate
							ORDER BY dt.SchoolDate
						),		
			s.AssessmentKey =(
								SELECT TOP (1) da.AssessmentKey
								FROM dbo.DimAssessment da
								WHERE s._sourceAssessmentKey = da._sourceKey									
									AND s.[ModifiedDate] >= da.[ValidFrom]
									AND s.[ModifiedDate] < da.[ValidTo]
								ORDER BY da.[ValidFrom] DESC
						 	 )  										             
        FROM Staging.StudentAssessmentScore s;
		
		DELETE FROM Staging.StudentAssessmentScore
		--SELECT * FROM Staging.StudentAssessmentScore
		WHERE StudentKey IS NULL OR 
		      TimeKey IS NULL OR			  
			  AssessmentKey IS NULL;

		--dropping the columnstore index
        DROP INDEX IF EXISTS CSI_FactStudentAssessmentScore ON dbo.FactStudentAssessmentScore;
	    
		--deleting changed records
		DELETE prod
		FROM [dbo].FactStudentAssessmentScore AS prod
		WHERE EXISTS (SELECT 1 
		              FROM [Staging].StudentAssessmentScore stage
					  WHERE prod._sourceKey = stage._sourceKey)



		 INSERT INTO [dbo].[FactStudentAssessmentScore]
						   ([_sourceKey]
						   ,[StudentKey]
						   ,[TimeKey]
						   ,[AssessmentKey]
						   ,ScoreResult
						   ,IntegerScoreResult
						   ,DecimalScoreResult
						   ,LiteralScoreResult
						   ,[LineageKey])
		SELECT DISTINCT 
		   [_sourceKey],
		   [StudentKey],
		   [TimeKey],
		   [AssessmentKey],
		   ScoreResult,
		   IntegerScoreResult,
		   DecimalScoreResult,
		   LiteralScoreResult,
		   	@lineageKey AS LineageKey
		FROM Staging.StudentAssessmentScore

		--loading from legacy dw just once
		IF (NOT EXISTS(SELECT 1  
		               FROM dbo.FactStudentAssessmentScore 
		               WHERE _sourceKey = 'LegacyDW'))
             BEGIN		
			       ;WITH UnpivotedScores AS 
					(
						SELECT testid,schyear,testtime,studentno,adminyear,grade, scoretype, scorevalue,lastupdate, CASE WHEN a.scoretype IN ('Proficiency level','Proficiency level 2') THEN 1 ELSE 0 END AS isperflevel
						--INTO [Raw_LegacyDW].[MCASAssessmentScores]
						FROM (  
								 --ensuring all score columns have the same data type to avoid conflicts with unpivot
								 SELECT testid,schyear,testtime,studentno,adminyear,grade,teststatus , lastupdate,
									  CAST(rawscore AS NVARCHAR(MAX)) AS [Raw score],
									  CAST(scaledscore AS NVARCHAR(MAX)) AS [Scale score],
									  CAST(perflevel AS NVARCHAR(MAX)) AS  [Proficiency level],
									  CAST(sgp AS NVARCHAR(MAX)) AS [Percentile rank],
									  CAST(cpi AS NVARCHAR(MAX)) AS [Composite Performance Index],
									  CAST(perf2 AS NVARCHAR(MAX)) AS [Proficiency level 2]
     
								FROM [BPSGranary02].[RAEDatabase].[dbo].[mcasitems] 
								WHERE schyear >= 2015 ) scores
						UNPIVOT
						(
						   scorevalue
						   FOR scoretype IN ([Raw score],[Scale score],[Proficiency level],[Percentile rank],[Composite Performance Index],[Proficiency level 2])
						) AS a

					)
				   INSERT INTO [dbo].[FactStudentAssessmentScore]
						   ([_sourceKey]
						   ,[StudentKey]
						   ,[TimeKey]
						   ,[AssessmentKey]
						   ,ScoreResult
						   ,IntegerScoreResult
						   ,DecimalScoreResult
						   ,LiteralScoreResult
						   ,[LineageKey])

					SELECT   DISTINCT 
						  'LegacyDW',
						  ds.StudentKey,
						  dt.TimeKey,	  
						  da.AssessmentKey,
						  us.scorevalue AS [SoreResult],
						  CASE when da.ResultDatatypeTypeDescriptor_CodeValue in ('Integer') AND TRY_CAST(us.scorevalue AS INTEGER) IS NOT NULL AND us.scorevalue <> '-' THEN us.scorevalue ELSE NULL END AS IntegerScoreResult,
						  CASE when da.ResultDatatypeTypeDescriptor_CodeValue in ('Decimal','Percentage','Percentile')  AND TRY_CAST(us.scorevalue AS FLOAT)  IS NOT NULL THEN us.scorevalue ELSE NULL END AS DecimalScoreResult,
						  CASE when da.ResultDatatypeTypeDescriptor_CodeValue not in ('Integer','Decimal','Percentage','Percentile') THEN us.scorevalue ELSE NULL END AS LiteralScoreResult,
						  --us.*
						  @lineageKey AS [LineageKey]
					--select top 100 *  
					FROM UnpivotedScores us

						--joining DW tables
						INNER JOIN dbo.DimTime dt ON CONVERT(DATE ,CASE WHEN SUBSTRING(us.testid,LEN(us.testid)-1,1) IN ('E','X') THEN DATEADD(YEAR, CASE WHEN MONTH(us.lastupdate) >= 7 THEN us.schyear ELSE us.schyear + 1 end  - YEAR(us.lastupdate), us.lastupdate)
																						WHEN us.testid = 'MCAS03AE' and us.testtime = 'S' and us.schyear = '2015' then '3/21/2016'
																						WHEN us.testid = 'MCAS03AM' and us.testtime = 'S' and us.schyear = '2015' then '5/9/2016'
																						WHEN us.testid = 'MCAS04AE' and us.testtime = 'S' and us.schyear = '2015' then '3/22/2016'
																						WHEN us.testid = 'MCAS04AM' and us.testtime = 'S' and us.schyear = '2015' then '5/9/2016'
																						WHEN us.testid = 'MCAS05AE' and us.testtime = 'S' and us.schyear = '2015' then '3/21/2016'
																						WHEN us.testid = 'MCAS05AM' and us.testtime = 'S' and us.schyear = '2015' then '5/9/2016'
																						WHEN us.testid = 'MCAS05AS' and us.testtime = 'S' and us.schyear = '2015' then '5/10/2016'
																						WHEN us.testid = 'MCAS06AE' and us.testtime = 'S' and us.schyear = '2015' then '3/21/2016'
																						WHEN us.testid = 'MCAS06AM' and us.testtime = 'S' and us.schyear = '2015' then '5/9/2016'
																						WHEN us.testid = 'MCAS07AE' and us.testtime = 'S' and us.schyear = '2015' then '3/22/2016'
																						WHEN us.testid = 'MCAS07AM' and us.testtime = 'S' and us.schyear = '2015' then '5/9/2016'
																						WHEN us.testid = 'MCAS08AE' and us.testtime = 'S' and us.schyear = '2015' then '3/21/2016'
																						WHEN us.testid = 'MCAS08AM' and us.testtime = 'S' and us.schyear = '2015' then '5/9/2016'
																						WHEN us.testid = 'MCAS08AS' and us.testtime = 'S' and us.schyear = '2015' then '5/10/2016'
																						WHEN us.testid = 'MCAS10AE' and us.testtime = 'S' and us.schyear = '2015' then '3/22/2016'
																						WHEN us.testid = 'MCAS10AM' and us.testtime = 'S' and us.schyear = '2015' then '3/23/2016'
																						WHEN us.testid = 'MCAS10BE' and us.testtime = 'F' and us.schyear = '2015' then '11/4/2015'
																						WHEN us.testid = 'MCAS10BE' and us.testtime = 'W' and us.schyear = '2015' then '3/2/2016'
																						WHEN us.testid = 'MCAS10BM' and us.testtime = 'F' and us.schyear = '2015' then '11/9/2015'
																						WHEN us.testid = 'MCAS10BM' and us.testtime = 'W' and us.schyear = '2015' then '3/2/2016'
																						WHEN us.testid = 'MCASHSAB' and us.testtime = 'W' and us.schyear = '2015' then '2/1/2016'
																						WHEN us.testid = 'MCASHSAB' and us.testtime = 'S' and us.schyear = '2015' then '6/1/2016'
																						WHEN us.testid = 'MCASHSAC' and us.testtime = 'S' and us.schyear = '2015' then '6/1/2016'
																						WHEN us.testid = 'MCASHSAP' and us.testtime = 'S' and us.schyear = '2015' then '6/1/2016'
																						WHEN us.testid = 'MCASHSAT' and us.testtime = 'S' and us.schyear = '2015' then '6/1/2016'
																						WHEN us.testid = 'MCAS03AE' and us.testtime = 'S' and us.schyear = '2016' then '4/3/2017'
																						WHEN us.testid = 'MCAS03AM' and us.testtime = 'S' and us.schyear = '2016' then '4/4/2017'
																						WHEN us.testid = 'MCAS04AE' and us.testtime = 'S' and us.schyear = '2016' then '4/3/2017'
																						WHEN us.testid = 'MCAS04AM' and us.testtime = 'S' and us.schyear = '2016' then '4/4/2017'
																						WHEN us.testid = 'MCAS05AE' and us.testtime = 'S' and us.schyear = '2016' then '4/3/2017'
																						WHEN us.testid = 'MCAS05AM' and us.testtime = 'S' and us.schyear = '2016' then '4/4/2017'
																						WHEN us.testid = 'MCAS05AS' and us.testtime = 'S' and us.schyear = '2016' then '4/5/2017'
																						WHEN us.testid = 'MCAS06AE' and us.testtime = 'S' and us.schyear = '2016' then '4/3/2017'
																						WHEN us.testid = 'MCAS06AM' and us.testtime = 'S' and us.schyear = '2016' then '4/4/2017'
																						WHEN us.testid = 'MCAS07AE' and us.testtime = 'S' and us.schyear = '2016' then '4/3/2017'
																						WHEN us.testid = 'MCAS07AM' and us.testtime = 'S' and us.schyear = '2016' then '4/4/2017'
																						WHEN us.testid = 'MCAS08AE' and us.testtime = 'S' and us.schyear = '2016' then '4/3/2017'
																						WHEN us.testid = 'MCAS08AM' and us.testtime = 'S' and us.schyear = '2016' then '4/4/2017'
																						WHEN us.testid = 'MCAS08AS' and us.testtime = 'S' and us.schyear = '2016' then '4/5/2017'
																						WHEN us.testid = 'MCAS10AE' and us.testtime = 'S' and us.schyear = '2016' then '3/21/2017'
																						WHEN us.testid = 'MCAS10AM' and us.testtime = 'S' and us.schyear = '2016' then '5/16/2017'
																						WHEN us.testid = 'MCAS10BE' and us.testtime = 'F' and us.schyear = '2016' then '11/2/2016'
																						WHEN us.testid = 'MCAS10BE' and us.testtime = 'W' and us.schyear = '2016' then '3/1/2017'
																						WHEN us.testid = 'MCAS10BM' and us.testtime = 'F' and us.schyear = '2016' then '11/9/2016'
																						WHEN us.testid = 'MCAS10BM' and us.testtime = 'W' and us.schyear = '2016' then '3/1/2017'
																						WHEN us.testid = 'MCASHSAB' and us.testtime = 'W' and us.schyear = '2016' then '2/6/2017'
																						WHEN us.testid = 'MCASHSAB' and us.testtime = 'S' and us.schyear = '2016' then '6/5/2017'
																						WHEN us.testid = 'MCASHSAC' and us.testtime = 'S' and us.schyear = '2016' then '6/5/2017'
																						WHEN us.testid = 'MCASHSAP' and us.testtime = 'S' and us.schyear = '2016' then '6/5/2017'
																						WHEN us.testid = 'MCASHSAT' and us.testtime = 'S' and us.schyear = '2016' then '6/5/2017'
																						WHEN us.testid = 'MCAS03AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																						WHEN us.testid = 'MCAS03AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																						WHEN us.testid = 'MCAS04AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																						WHEN us.testid = 'MCAS04AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																						WHEN us.testid = 'MCAS05AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																						WHEN us.testid = 'MCAS05AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																						WHEN us.testid = 'MCAS05AS' and us.testtime = 'S' and us.schyear = '2017' then '4/4/2018'
																						WHEN us.testid = 'MCAS06AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																						WHEN us.testid = 'MCAS06AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																						WHEN us.testid = 'MCAS07AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																						WHEN us.testid = 'MCAS07AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																						WHEN us.testid = 'MCAS08AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																						WHEN us.testid = 'MCAS08AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																						WHEN us.testid = 'MCAS08AS' and us.testtime = 'S' and us.schyear = '2017' then '4/4/2018'
																						WHEN us.testid = 'MCAS10AE' and us.testtime = 'S' and us.schyear = '2017' then '3/27/2018'
																						WHEN us.testid = 'MCAS10AM' and us.testtime = 'S' and us.schyear = '2017' then '5/23/2018'
																						WHEN us.testid = 'MCAS10BE' and us.testtime = 'F' and us.schyear = '2017' then '11/8/2017'
																						WHEN us.testid = 'MCAS10BE' and us.testtime = 'W' and us.schyear = '2017' then '2/28/2018'
																						WHEN us.testid = 'MCAS10BM' and us.testtime = 'W' and us.schyear = '2017' then '2/28/2018'
																						WHEN us.testid = 'MCAS10BM' and us.testtime = 'F' and us.schyear = '2017' then '11/15/2017'
																						WHEN us.testid = 'MCAS3 AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																						WHEN us.testid = 'MCAS3 AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																						WHEN us.testid = 'MCAS4 AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																						WHEN us.testid = 'MCAS4 AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																						WHEN us.testid = 'MCAS5 AE' and us.testtime = 'S' and us.schyear = '2017' then '4/2/2018'
																						WHEN us.testid = 'MCAS5 AM' and us.testtime = 'S' and us.schyear = '2017' then '4/3/2018'
																						WHEN us.testid = 'MCAS5 AS' and us.testtime = 'S' and us.schyear = '2017' then '4/4/2018'
																						WHEN us.testid = 'MCASHSAB' and us.testtime = 'S' and us.schyear = '2017' then '2/1/2018'
																						WHEN us.testid = 'MCASHSAB' and us.testtime = 'W' and us.schyear = '2017' then '6/1/2018'
																						WHEN us.testid = 'MCASHSAC' and us.testtime = 'S' and us.schyear = '2017' then '6/1/2018'
																						WHEN us.testid = 'MCASHSAP' and us.testtime = 'S' and us.schyear = '2017' then '6/1/2018'
																						WHEN us.testid = 'MCASHSAT' and us.testtime = 'S' and us.schyear = '2017' then '6/1/2018'
																						ELSE '1900/01/01'
																				   END ) = dt.SchoolDate
	                               
						INNER JOIN dbo.DimStudent ds  ON 'LegacyDW|' + Convert(NVARCHAR(MAX),us.studentno)   = ds._sourceKey
																	  AND dt.SchoolKey = ds.SchoolKey
																	  AND dt.SchoolDate BETWEEN ds.ValidFrom AND ds.ValidTo
						INNER JOIN [dbo].DimAssessment da ON 'LegacyDW|' + Convert(NVARCHAR(MAX),us.testid)  + '|N/A|' + Convert(NVARCHAR(MAX),us.scoretype)  = da._sourceKey
	
					WHERE  dt.SchoolDate BETWEEN '07/01/2015' AND '2018-06-30';

			 END;
		
        --re-creating the columnstore index
		CREATE COLUMNSTORE INDEX CSI_FactStudentAssessmentScore
			  ON dbo.FactStudentAssessmentScore
			  ([StudentKey]
			  ,[TimeKey]
			  ,[AssessmentKey]
			  ,[ScoreResult]
			  ,[IntegerScoreResult]
			  ,[DecimalScoreResult]
			  ,[LiteralScoreResult]
			  ,[LineageKey])

		--Deriving
		--dropping the columnstore index
		DROP INDEX IF EXISTS CSI_Derived_StudentAssessmentScore ON Derived.StudentAssessmentScore;
		
		delete d_sas
		FROM  [Derived].[StudentAssessmentScore] d_sas
		WHERE EXISTS(SELECT 1 
		             FROM Staging.StudentAssessmentScore s_sas
					 WHERE d_sas.StudentKey = s_sas.StudentKey
					    AND d_sas.[TimeKey] = s_sas.[TimeKey]
						AND d_sas.[AssessmentKey] =  s_sas.[AssessmentKey])
						
		INSERT INTO [Derived].[StudentAssessmentScore]
					([StudentKey]
					,[TimeKey]
					,[AssessmentKey]
					,[AchievementProficiencyLevel]
					,[CompositeRating]
					,[CompositeScore]
					,[PercentileRank]
					,[ProficiencyLevel]
					,[PromotionScore]
					,[RawScore]
					,[ScaleScore])
    
		SELECT  [StudentKey],
				[TimeKey],
				[AssessmentKey],
				--pivoted from row values
				[Achievement/proficiency level] AS AchievementProficiencyLevel ,
				[Composite Rating] AS CompositeRating,
				[Composite Score] AS CompositeScore,
				[Percentile rank] AS PercentileRank,
				[Proficiency level] AS ProficiencyLevel,
				[Promotion score] AS PromotionScore,
				[Raw score] AS RawScore,
				[Scale score] AS ScaleScore
		FROM (
				SELECT  
				        fas.[StudentKey],
						fas.[TimeKey],
						fas.[AssessmentKey],
						da.[ReportingMethodDescriptor_CodeValue] AS ScoreType,
						fas.ScoreResult AS Score
				FROM dbo.FactStudentAssessmentScore fas  
						INNER JOIN dbo.DimAssessment da ON fas.AssessmentKey = da.AssessmentKey
				WHERE EXISTS(SELECT 1 
							 FROM Staging.StudentAssessmentScore s_sas
							 WHERE fas.StudentKey = s_sas.StudentKey
								AND fas.[TimeKey] = s_sas.[TimeKey]
								AND fas.[AssessmentKey] =  s_sas.[AssessmentKey])
			 
			) AS SourceTable 
		PIVOT 
			(
				MAX(Score)
				FOR ScoreType IN ([Achievement/proficiency level],
								[Composite Rating],[Composite Score],
								[Percentile rank],
								[Proficiency level],
								[Promotion score],
								[Raw score],
								[Scale score])
			) AS PivotTable;


		CREATE COLUMNSTORE INDEX CSI_Derived_StudentAssessmentScore
			ON Derived.StudentAssessmentScore
			([StudentKey]
			,[TimeKey]
			,[AssessmentKey]
			,[AchievementProficiencyLevel]
			,[CompositeRating]
			,[CompositeScore]
			,[PercentileRank]
			,[ProficiencyLevel]
			,[PromotionScore]
			,[RawScore]
			,[ScaleScore])


		-- updating the EndTime to now and status to Success		
		UPDATE dbo.ETL_Lineage
			SET 
				EndTime = SYSDATETIME(),
				Status = 'S' -- success
		WHERE [LineageKey] = @LineageKey;
	
	
		-- Update the LoadDates table with the most current load date
		UPDATE [dbo].[ETL_IncrementalLoads]
		SET [LoadDate] = @LastDateLoaded
		WHERE [TableName] = N'dbo.FactStudentAssessmentScore';

		
	    
		COMMIT TRANSACTION;		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		IF XACT_STATE( ) = -1
			BEGIN
				--The transaction is in an uncommittable state. Rolling back transaction
				ROLLBACK TRANSACTION;
			END;

		-- Test whether the transaction is committable.
		IF XACT_STATE( ) = 1
			BEGIN
				--The transaction is committable. Committing transaction
				COMMIT TRANSACTION;
			END;
	END CATCH;
END;
GO


--Fact StudentCourseTranscript
----------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_FactStudentCourseTranscript_PopulateStaging] 
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY

		--DECLARE @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate();
		--select * from Staging.StudentCourseTranscript
		TRUNCATE TABLE Staging.StudentCourseTranscript	
		INSERT INTO Staging.StudentCourseTranscript
		(
		    _sourceKey,
		    StudentKey,
		    TimeKey,
		    CourseKey,
		    SchoolKey,
		    EarnedCredits,
		    PossibleCredits,
		    FinalLetterGradeEarned,
		    FinalNumericGradeEarned,
		    ModifiedDate,
		    _sourceStudentKey,
		    _sourceSchoolYear,
			_sourceTerm,
		    _sourceCourseKey,
		    _sourceSchoolKey
		)
		
		SELECT DISTINCT
			   CONCAT_WS('|',Convert(NVARCHAR(MAX),ct.StudentUSI),Convert(NVARCHAR(MAX),ct.SchoolYear),Convert(NVARCHAR(MAX),ct.SchoolId),Convert(NVARCHAR(MAX),ct.CourseCode),td.CodeValue) AS _sourceKey,
			   NULL AS StudentKey,
			   NULL AS TimeKey,	  
			   NULL AS CourseKey,
			   NULL AS SchoolKey,  
			   ct.EarnedCredits,
			   ct.AttemptedCredits,
			   ct.FinalLetterGradeEarned,
			   ct.FinalNumericGradeEarned,			   
			   ct.LastModifiedDate AS ModifiedDate,
			   CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),ct.StudentUSI)) AS _sourceStudentKey,
		       ct.SchoolYear AS _sourceSchoolYear,		          
			   td.CodeValue AS _sourceTerm,		          
			   CONCAT_WS('|','Ed-Fi',ct.CourseCode)  AS _sourceCourseKey,
			   CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),ct.SchoolId))  AS _sourceSchoolKey
		--select *  
		FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseTranscript ct
			INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor td ON ct.TermDescriptorId = td.DescriptorId
		WHERE  			
			  ct.SchoolYear >= 2019			
			 AND  (
					   (ct.LastModifiedDate > @LastLoadDate  AND ct.LastModifiedDate <= @NewLoadDate)			     
				  )
		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

	END CATCH;
END; 
GO
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_FactStudentCourseTranscript_PopulateProduction]
@LineageKey INT,
@LastDateLoaded DATETIME
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	    
		BEGIN TRANSACTION;   
		 

		--updating staging keys
		UPDATE s 
		SET s.StudentKey = (
								SELECT TOP (1) ds.StudentKey
								FROM dbo.DimStudent ds
								WHERE s._sourceStudentKey = ds._sourceKey									
									AND s.[ModifiedDate] >= ds.[ValidFrom]
									AND s.[ModifiedDate] < ds.[ValidTo]
								ORDER BY ds.[ValidFrom] DESC
							),
			s.SchoolKey = (
								SELECT TOP (1) ds.SchoolKey
								FROM dbo.DimSchool ds
								WHERE s._sourceSchoolKey = ds._sourceKey									
									AND s.[ModifiedDate] >= ds.[ValidFrom]
									AND s.[ModifiedDate] < ds.[ValidTo]
								ORDER BY ds.[ValidFrom] DESC
							),
			s.CourseKey = (
								SELECT TOP (1) dc.CourseKey
								FROM dbo.DimCourse dc
								WHERE s._sourceCourseKey = dc._sourceKey									
									AND s.[ModifiedDate] >= dc.[ValidFrom]
									AND s.[ModifiedDate] < dc.[ValidTo]
								ORDER BY dc.[ValidFrom] DESC
							)													             
        FROM Staging.StudentCourseTranscript s;

		--updating the timeKey
		--this is a special case since we need the min date term
		;WITH SchoolTermsFirstDates AS 
			(
			  SELECT DISTINCT dt.SchoolKey, 
							  dt.SchoolTermDescriptor_CodeValue AS Term,
							  dt.SchoolYear,
							  MIN(dt.SchoolDate) OVER (PARTITION BY dt.SchoolKey, dt.SchoolTermDescriptor_CodeValue, dt.SchoolYear) AS MinTermDate
			  FROM dbo.DimTime dt
			       INNER JOIN dbo.DimSchool ds ON dt.SchoolKey = ds.SchoolKey
			  WHERE dt.SchoolKey IS NOT NULL
			        AND EXISTS (SELECT 1 
					            FROM Staging.StudentCourseTranscript sct
								WHERE ds._sourceKey = sct._sourceSchoolKey
								  AND dt.SchoolYear = sct._sourceSchoolYear
								  AND dt.SchoolTermDescriptor_CodeValue = sct._sourceTerm )
			)

		UPDATE s 
		SET s.TimeKey = ( SELECT TOP (1) dt.TimeKey
						  FROM dbo.DimTime dt									
						  WHERE dt.SchoolTermDescriptor_CodeValue = s._sourceTerm
							      AND dt.SchoolYear = s._sourceSchoolYear
								  AND dt.SchoolKey = s.SchoolKey
							      AND EXISTS ( SELECT 1
												FROM SchoolTermsFirstDates std 
												WHERE dt.SchoolKey = std.SchoolKey
												   AND dt.SchoolTermDescriptor_CodeValue = std.Term
												   AND dt.SchoolYear = std.SchoolYear
												   AND dt.SchoolDate = std.MinTermDate )
							ORDER BY dt.SchoolDate )
         FROM Staging.StudentCourseTranscript s;


		DELETE FROM Staging.StudentCourseTranscript
		--select * FROM Staging.StudentCourseTranscript
		WHERE StudentKey IS NULL OR 
		      TimeKey IS NULL OR
			  SchoolKey IS NULL OR
			  CourseKey IS NULL;
		
		--dropping the columnstore index
        DROP INDEX IF EXISTS CSI_FactStudentCourseTranscript ON dbo.FactStudentCourseTranscript;		   

		--deleting changed records
		DELETE prod
		FROM [dbo].FactStudentCourseTranscript AS prod
		WHERE EXISTS (SELECT 1 
		              FROM [Staging].StudentCourseTranscript stage
					  WHERE prod._sourceKey = stage._sourceKey)

         INSERT INTO [dbo].FactStudentCourseTranscript
					   ([_sourceKey]
					    ,[StudentKey]
						,[TimeKey]
						,[CourseKey]
						,[SchoolKey]
						,EarnedCredits
						,PossibleCredits 
						,FinalLetterGradeEarned
						,FinalNumericGradeEarned
						,[LineageKey])
		SELECT DISTINCT 
		      [_sourceKey]
			 ,[StudentKey]
			 ,[TimeKey]
			 ,[CourseKey]
			 ,[SchoolKey]
			 ,EarnedCredits
			 ,PossibleCredits 
			 ,FinalLetterGradeEarned
			 ,FinalNumericGradeEarned
			 ,@LineageKey AS [LineageKey]
        FROM Staging.StudentCourseTranscript
		
		IF (NOT EXISTS(SELECT 1  
		               FROM dbo.FactStudentCourseTranscript 
		               WHERE _sourceKey = 'LegacyDW'))
             BEGIN		
			   --legacy 			
				;WITH SchoolTermsFirstDates AS 
				(
				  SELECT DISTINCT SchoolKey, 
								  SchoolTermDescriptor_CodeValue AS Term,
								  SchoolYear,
								  MIN(SchoolDate) OVER (PARTITION BY SchoolKey, SchoolTermDescriptor_CodeValue, SchoolYear) AS MinTermDate
				  FROM dbo.DimTime 
				  WHERE SchoolKey IS NOT NULL

				)
				INSERT INTO [dbo].FactStudentCourseTranscript
						   ([_sourceKey]
							,[StudentKey]
							,[TimeKey]
							,[CourseKey]
							,[SchoolKey]
							,EarnedCredits
							,PossibleCredits 
							,FinalLetterGradeEarned
							,FinalNumericGradeEarned
							,[LineageKey])
		    
				SELECT DISTINCT
						'LegacyDW',
						ds.StudentKey,
						dt.TimeKey,
						dcourse.CourseKey,	  
						dschool.SchoolKey,      
						COALESCE(scg.CreditsEarned,0) AS EarnedCredits,
						scg.CreditsPossible AS PossibleCredits,
						CASE WHEN scg.CreditsEarned = 0 AND scg.FinalMark IS NULL THEN 'NC'
						ELSE
							CASE WHEN TRY_CAST(scg.FinalMark AS DECIMAL) IS NULL THEN scg.FinalMark 
							ELSE NULL 
							END 
						END AS FinalLetterGradeEarned,
						CASE WHEN TRY_CAST(scg.FinalMark AS DECIMAL) IS NOT NULL THEN scg.FinalMark ELSE NULL END AS FinalNumericGradeEarned,
						--dt.SchoolDate, *
						@lineageKey AS [LineageKey]
				--select *  
				FROM [BPSGranary02].[RAEDatabase].[dbo].[StudentCourseGrade_aspenNewFormat] scg
    
					--joining DW tables
					INNER JOIN EdFiDW.dbo.DimStudent ds  ON CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),scg.StudentNo))  = ds._sourceKey
	      
					INNER JOIN EdFiDW.dbo.DimSchool dschool ON CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),scg.SchoolID))   = dschool._sourceKey
					INNER JOIN EdFiDW.dbo.DimCourse dcourse ON CONCAT('LegacyDW|',scg.CourseNumber,'-',CASE WHEN scg.SectionID = '' THEN 'N/A' ELSE scg.SectionID END)  = dcourse._sourceKey	
					INNER JOIN EdFiDW.dbo.DimTime dt on dt.SchoolKey is not null   
													and dschool.SchoolKey = dt.SchoolKey
													and CASE WHEN scg.Semester IN ('A','MS Pre-Algebra','Advisory') AND dt.SchoolTermDescriptor_CodeValue = 'Other' THEN 1									         
																WHEN scg.Semester IN ('SS') AND dt.SchoolTermDescriptor_CodeValue = 'Summer Semester' THEN 1
																WHEN scg.Semester IN ('1') AND dt.SchoolTermDescriptor_CodeValue = 'Fall Semester' THEN 1
																WHEN scg.Semester IN ('2') AND dt.SchoolTermDescriptor_CodeValue = 'Spring Semester' THEN 1
										
																WHEN scg.Semester IN ('Q1') AND dt.SchoolTermDescriptor_CodeValue = 'First Quarter' THEN 1
																WHEN scg.Semester IN ('Q123','Q13','Q14','Q23','Q24','T124','T234') AND dt.SchoolTermDescriptor_CodeValue = 'Other' THEN 1											 
																WHEN scg.Semester IN ('Q2') AND dt.SchoolTermDescriptor_CodeValue = 'Second Quarter' THEN 1
																WHEN scg.Semester IN ('Q3') AND dt.SchoolTermDescriptor_CodeValue = 'Third Quarter' THEN 1
																WHEN scg.Semester IN ('Q4','T4') AND dt.SchoolTermDescriptor_CodeValue = 'Fourth Quarter' THEN 1											 

											 

																WHEN scg.Semester IN ('T1') AND scg.SchoolID IN ('4580','1064','1420') AND dt.SchoolTermDescriptor_CodeValue = 'First Quarter' THEN 1
																WHEN scg.Semester IN ('T1') AND dt.SchoolTermDescriptor_CodeValue = 'First Trimester' THEN 1

																WHEN scg.Semester IN ('T2') AND scg.SchoolID IN ('4580','1064','1420') AND dt.SchoolTermDescriptor_CodeValue = 'Second Quarter' THEN 1
																WHEN scg.Semester IN ('T2') AND dt.SchoolTermDescriptor_CodeValue = 'Second Trimester' THEN 1

																WHEN scg.Semester IN ('T3') AND scg.SchoolID IN ('4580','1064','1420') AND dt.SchoolTermDescriptor_CodeValue = 'Third Quarter' THEN 1
																WHEN scg.Semester IN ('T3') AND dt.SchoolTermDescriptor_CodeValue = 'Third Trimester' THEN 1
																ELSE 0
														END = 1
													AND RIGHT(RTRIM(scg.SchoolYear),4) = dt.SchoolYear

				WHERE   dt.SchoolDate BETWEEN ds.ValidFrom  AND ds.ValidTo
					AND dt.SchoolDate BETWEEN dschool.ValidFrom AND dschool.ValidTo
					AND dt.SchoolDate BETWEEN dcourse.ValidFrom AND dcourse.ValidTo
					AND scg.SchoolYear IN ('2015-2016','2016-2017', '2017-2018')	   
					AND scg.FinalMark IS NOT NULL
						AND EXISTS (SELECT 1
								FROM SchoolTermsFirstDates std 
								WHERE dschool.SchoolKey = std.SchoolKey
									AND dt.SchoolTermDescriptor_CodeValue = std.Term
									AND dt.SchoolYear = std.SchoolYear
									AND dt.SchoolDate = std.MinTermDate)
						AND COALESCE(scg.CreditsEarned,0) > 0
			 END;

		--re-creating the columnstore index			
		CREATE COLUMNSTORE INDEX CSI_FactStudentCourseTranscript
			ON dbo.FactStudentCourseTranscript
			([StudentKey]
			,[TimeKey]
			,[CourseKey]
			,[SchoolKey]
			,[EarnedCredits]
			,[PossibleCredits]
			,[FinalLetterGradeEarned]
			,[FinalNumericGradeEarned]
			,[LineageKey])
			
		-- updating the EndTime to now and status to Success		
		UPDATE dbo.ETL_Lineage
		SET 
			EndTime = SYSDATETIME(),
			Status = 'S' -- success
		WHERE [LineageKey] = @LineageKey;
	
	
		-- Update the LoadDates table with the most current load date
		UPDATE [dbo].[ETL_IncrementalLoads]
		SET [LoadDate] = @LastDateLoaded
		WHERE [TableName] = N'dbo.FactStudentCourseTranscript';

		
	    
		COMMIT TRANSACTION;		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		IF XACT_STATE( ) = -1
			BEGIN
				--The transaction is in an uncommittable state. Rolling back transaction
				ROLLBACK TRANSACTION;
			END;

		-- Test whether the transaction is committable.
		IF XACT_STATE( ) = 1
			BEGIN
				--The transaction is committable. Committing transaction
				COMMIT TRANSACTION;
			END;
	END CATCH;
END;
GO


