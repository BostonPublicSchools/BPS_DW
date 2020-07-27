USE LongitudinalPOC
GO

DECLARE @dropExistingTables BIT = 1

IF (@dropExistingTables = 1)
BEGIN



  --schemas 
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
  
  ---------------------------------------------------------------

  --fact tables
  if exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentAttendanceByDay' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.FactStudentAttendanceByDay; 

  if exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentAttendanceBySection' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.FactStudentAttendanceBySection; 
    
  if exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentAssessmentScore' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.FactStudentAssessmentScore; 

  if exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentDiscipline' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.FactStudentDiscipline; 

  if exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentCourseTranscript' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.FactStudentCourseTranscript; 
	  
  --dim table
  IF exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimStaff' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.DimStaff; 

  if exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimCourse' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.DimCourse; 

  if exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimAssessment' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.DimAssessment; 

  IF exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimSection' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.DimSection; 

    if exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimDisciplineIncident' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.DimDisciplineIncident; 
	  

  if exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimDisciplineIncidentBehavior' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.DimDisciplineIncidentBehavior; 

  if exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimDisciplineIncidentLocation' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.DimDisciplineIncidentLocation; 

  if exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimDisciplineIncidentAction' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.DimDisciplineIncidentAction; 

  if exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimDisciplineIncidentReporterType' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.DimDisciplineIncidentReporterType; 

  if exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimAttendanceEventCategory' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.DimAttendanceEventCategory; 
  
  if exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimAttendanceEventCategory' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.DimAttendanceEventCategory; 

   IF exists (select 1
					FROM INFORMATION_SCHEMA.TABLES
					WHERE TABLE_NAME = 'DimStudent' 
					AND TABLE_SCHEMA = 'dbo')
			DROP TABLE dbo.DimStudent; 

   IF exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimTime' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.DimTime; 

   IF exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimSchool' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.DimSchool;

   IF exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimSchool' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP TABLE dbo.DimSchool;
 
   IF exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'School' 
			   AND TABLE_SCHEMA = 'Staging')
      DROP TABLE Staging.School;
	   
   --integration tables
   IF exists (select 1
				FROM INFORMATION_SCHEMA.TABLES
				WHERE TABLE_NAME = 'Lineage' 
				AND TABLE_SCHEMA = 'dbo')
		DROP TABLE dbo.Lineage; 

   IF exists (select 1
				FROM INFORMATION_SCHEMA.TABLES
				WHERE TABLE_NAME = 'IncrementalLoads' 
				AND TABLE_SCHEMA = 'dbo')
		DROP TABLE dbo.IncrementalLoads; 
		    
   --views
   IF exists (select 1
             FROM INFORMATION_SCHEMA.VIEWS
             WHERE TABLE_NAME = 'View_StudentAssessmentScores' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP VIEW dbo.View_StudentAssessmentScores; 

   IF exists (select 1
             FROM INFORMATION_SCHEMA.VIEWS
             WHERE TABLE_NAME = 'View_StudentAttendanceByDay' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP VIEW dbo.View_StudentAttendanceByDay; 

   IF exists (select 1
             FROM INFORMATION_SCHEMA.VIEWS
             WHERE TABLE_NAME = 'View_StudentDiscipline' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP VIEW dbo.View_StudentDiscipline; 

   IF exists (select 1
             FROM INFORMATION_SCHEMA.VIEWS
             WHERE TABLE_NAME = 'View_StudentCourseTranscript' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP VIEW dbo.View_StudentCourseTranscript; 

   IF exists (select 1
             FROM INFORMATION_SCHEMA.VIEWS
             WHERE TABLE_NAME = 'View_StudentRoster' 
			   AND TABLE_SCHEMA = 'dbo')
      DROP VIEW dbo.View_StudentRoster; 


END;


--Lineage
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'Lineage' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.Lineage
(
   LineageKey INT NOT NULL IDENTITY(1,1), -- surrogate
   TableName NVARCHAR(100) NOT NULL,
   StartTime DATETIME NOT NULL,
   EndTime DATETIME NULL,
   LoadType CHAR(1) NOT NULL , -- F = Full, I = Incremental
   [Status] CHAR(1) NOT NULL, -- P = Processing , S = Success
   CONSTRAINT PK_Lineage PRIMARY KEY (LineageKey),
);

if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'IncrementalLoads' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE [dbo].[IncrementalLoads](
	[LoadDateKey] [int] IDENTITY(1,1) NOT NULL,
	[TableName] [nvarchar](100) NOT NULL,
	[LoadDate] [datetime] NOT NULL,
 CONSTRAINT [PK_LoadDates] PRIMARY KEY CLUSTERED  ([LoadDateKey] ASC)
) ON [PRIMARY]


GO



--DIMENSION TABLES
-------------------------------------------------------------------------------------------------
--school
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimSchool' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.DimSchool
(
  SchoolKey int NOT NULL IDENTITY(1,1), -- ex 9/1/2019 : 20190901 -- surrogate
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

  CONSTRAINT PK_DimSchool PRIMARY KEY (SchoolKey),
  CONSTRAINT FK_DimSchool_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);


--time
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimTime' 
			   AND TABLE_SCHEMA = 'dbo')
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
  CONSTRAINT FK_DimTime_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES [dbo].[DimSchool] (SchoolKey),
  CONSTRAINT FK_DimTime_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);

--student
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimStudent' 
			   AND TABLE_SCHEMA = 'dbo')
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
	
	[RaceCode] [nvarchar](100) NOT NULL,
	[RaceDescription] [nvarchar](100) NOT NULL,
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
	[LimitedEnglishProficiencyDescriptor_CodeValue] [nvarchar](100) NOT NULL,
	[LimitedEnglishProficiencyDescriptor_Description] [nvarchar](100) NOT NULL,
	[LimitedEnglishProficiency_EnglishLearner_Indicator] [bit] NOT NULL,
	[LimitedEnglishProficiency_Former_Indicator] [bit] NOT NULL,
	[LimitedEnglishProficiency_NotEnglisLearner_Indicator] [bit] NOT NULL,
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

    CONSTRAINT PK_DimStudent PRIMARY KEY (StudentKey),
    CONSTRAINT FK_DimStudent_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey),
    CONSTRAINT FK_DimStudent_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);

--section
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimSection' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.DimSection
(
  SectionKey INT NOT NULL IDENTITY(1,1),
  [_sourceKey] NVARCHAR(50) NOT NULL,  --'Ed-Fi|25590100101Trad120ENG112011'
  
  [SchoolYear] [smallint] NOT NULL,  
  SchoolKey INT NOT NULL,
  ShortNameOfInstitution nvarchar(500) NOT NULL,
  NameOfInstitution nvarchar(500) NOT NULL,
  [ClassPeriodName] [nvarchar](20) NOT NULL,
  [ClassroomIdentificationCode] [nvarchar](20) NOT NULL,
  [LocalCourseCode] [nvarchar](60) NOT NULL,
  [SchoolTermDescriptor_CodeValue] nvarchar(50) not null,
  [SchoolTermDescriptor_Description] nvarchar(50) not null,
  
  ValidFrom DATETIME NOT NULL, 
  ValidTo DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL,    
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimSection PRIMARY KEY (SectionKey ASC),
  CONSTRAINT FK_DimSection_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey)
);

--staff
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
  
  [HispanicLatinoEthnicity_Indicator] [bit] NOT NULL,
  [OldEthnicityType_CodeValue] NVARCHAR(100)  NULL,
  [OldEthnicityType_Description] NVARCHAR(100)  NULL,
  [CitizenshipStatusType_CodeValue] NVARCHAR(100)  NULL,
  [CitizenshipStatusType_Description] NVARCHAR(100)  NULL,
  [HighestLevelOfEducationDescriptorDescriptor_CodeValue] NVARCHAR(100)  NULL, 
  [HighestLevelOfEducationDescriptorDescriptor_Description] NVARCHAR(100)  NULL, 
  [YearsOfPriorProfessionalExperience] [decimal](5, 2) NULL,
  [YearsOfPriorTeachingExperience] [decimal](5, 2) NULL,  
  [HighlyQualifiedTeacher_Indicator] [bit] NULL,
    
  ValidFrom DATETIME NOT NULL, 
  ValidTo DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL,    
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimStaff PRIMARY KEY (StaffKey)  
);

--discipline incident
--this is not a slowly changing dimension since there not a natural source key to match by
--we will deleting and repopulating the current year info
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimDisciplineIncident' 
			   AND TABLE_SCHEMA = 'dbo')
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
  [IncidentDescription] nvarchar(4000) NULL ,
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
  
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimDisciplineIncident PRIMARY KEY (DisciplineIncidentKey ASC)  ,
  CONSTRAINT FK_DimDisciplineIncident_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey)
);



--attendance event category
-- NOT USED FOR NOW
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimAttendanceEventCategory' 
			   AND TABLE_SCHEMA = 'dbo')
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


--discipline incident types
-- NOT USED FOR NOW
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimDisciplineIncidentBehavior' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.DimDisciplineIncidentBehavior
(
  DisciplineIncidentBehaviorKey INT NOT NULL IDENTITY(1,1),
  [_sourceKey] NVARCHAR(50) NOT NULL, 
  
  [BehaviorDescriptor_CodeValue] nvarchar(50) not null,
  [BehaviorDescriptor_Description] nvarchar(1024) not null,
  
  ValidFrom DATETIME NOT NULL, 
  ValidTo DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL,    
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimDisciplineIncidentBehavior PRIMARY KEY (DisciplineIncidentBehaviorKey ASC)  
);

--discipline incident location
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimDisciplineIncidentLocation' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.DimDisciplineIncidentLocation
(
  DisciplineIncidentLocationKey INT NOT NULL IDENTITY(1,1),
  [_sourceKey] NVARCHAR(50) NOT NULL, 
  
  [LocationDescriptor_CodeValue] nvarchar(50) not null,
  [LocationDescriptor_Description] nvarchar(1024) not null,
  
  ValidFrom DATETIME NOT NULL, 
  ValidTo DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL,    
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimDisciplineIncidentLocation PRIMARY KEY (DisciplineIncidentLocationKey  ASC)     
);

--discipline incident location
-- NOT USED FOR NOW
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimDisciplineIncidentAction' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.DimDisciplineIncidentAction
(
  DisciplineIncidentActionKey INT NOT NULL IDENTITY(1,1),
  [_sourceKey] NVARCHAR(50) NOT NULL, 
  
  [DisciplineDescriptor_CodeValue] nvarchar(50) not null,
  [DisciplineDescriptor_Description] nvarchar(1024) not null,
  DisciplineDescriptor_ISS_Indicator BIT NOT NULL,
  DisciplineDescriptor_OSS_Indicator BIT NOT NULL,
  ValidFrom DATETIME NOT NULL, 
  ValidTo DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL,    
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimDisciplineIncidentAction PRIMARY KEY (DisciplineIncidentActionKey ASC)  
);

--discipline incident reporter types
-- NOT USED FOR NOW
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimDisciplineIncidentReporterType' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.DimDisciplineIncidentReporterType
(
  DisciplineIncidentReporterTypeKey INT NOT NULL IDENTITY(1,1),
  [_sourceKey] NVARCHAR(50) NOT NULL, 
  
  ReporterDescriptor_CodeValue nvarchar(50) NOT NULL,
  ReporterDescriptor_Description nvarchar(1024) NOT NULL,
  
  ValidFrom DATETIME NOT NULL, 
  ValidTo DATETIME NOT NULL, 
  IsCurrent BIT NOT NULL,    
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_DimDisciplineIncidentReporterType PRIMARY KEY (DisciplineIncidentReporterTypeKey ASC)  
);



--FACT TABLES
----------------------------------------------------------------------
--attendance by day
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentAttendanceByDay' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.FactStudentAttendanceByDay
(
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
  CONSTRAINT FK_FactStudentAttendanceByDay_AttendanceEventCategoryKey FOREIGN KEY (AttendanceEventCategoryKey) REFERENCES dbo.DimAttendanceEventCategory(AttendanceEventCategoryKey),
  CONSTRAINT FK_FactStudentAttendanceByDay_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])  
);


--attendance by section
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentAttendanceBySection' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.FactStudentAttendanceBySection
(
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL,  
  SectionKey INT NOT NULL,
  StaffKey INT NOT NULL,
  SchoolKey INT NOT NULL,

  AttendanceEventCategoryKey INT NOT NULL ,
  AttendanceEventReason nvarchar(500) NOT NULL,

  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentAttendanceBySection PRIMARY KEY (StudentKey ASC, TimeKey ASC, SectionKey ASC,SchoolKey ASC, StaffKey ASC, AttendanceEventCategoryKey ASC),
  CONSTRAINT FK_FactStudentAttendanceBySection_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_SectionKey FOREIGN KEY (SectionKey) REFERENCES dbo.DimSection(SectionKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_StaffKey FOREIGN KEY (StaffKey) REFERENCES dbo.DimStaff(StaffKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_AttendanceEventCategoryKey FOREIGN KEY (AttendanceEventCategoryKey) REFERENCES dbo.DimAttendanceEventCategory(AttendanceEventCategoryKey),
  CONSTRAINT FK_FactStudentAttendanceBySection_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])  
);

--discipline -- v1
/*
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentDiscipline' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.FactStudentDiscipline
(
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL, 
  SchoolKey INT NOT NULL,
  IncidentTime TIME(7) NOT NULL, 
  
  DisciplineIncidentBehaviorKey INT NOT NULL, -- Weapons Possession (Firearms and Other Weapons), Drugs. etcs
  DisciplineIncidentLocationKey INT NOT NULL, -- Hallway, Cafeteria, Classroom, etc
  DisciplineIncidentActionKey INT NOT NULL, -- ISS, OSS
  DisciplineIncidentReporterTypeKey INT NOT NULL, --Law enforcement officer,Non-school personnel,Other,Parent/guardian,Staff,Student,


  IncidentReporterName NVARCHAR(100) NULL ,
  ReportedToLawEnforcement_Indicator BIT NOT NULL ,
  IncidentCost Money NOT NULL,
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentDiscipline PRIMARY KEY (StudentKey ASC, TimeKey ASC, SchoolKey ASC, IncidentTime ASC),
  CONSTRAINT FK_FactStudentDiscipline_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentDiscipline_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentDiscipline_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey),
  CONSTRAINT FK_FactStudentDiscipline_DisciplineIncidentBehaviorKey FOREIGN KEY (DisciplineIncidentBehaviorKey) REFERENCES dbo.DimDisciplineIncidentBehavior(DisciplineIncidentBehaviorKey),
  CONSTRAINT FK_FactStudentDiscipline_DisciplineIncidentLocationKey FOREIGN KEY (DisciplineIncidentLocationKey) REFERENCES dbo.DimDisciplineIncidentLocation(DisciplineIncidentLocationKey),
  CONSTRAINT FK_FactStudentDiscipline_DisciplineIncidentActionKey FOREIGN KEY (DisciplineIncidentActionKey) REFERENCES dbo.DimDisciplineIncidentAction(DisciplineIncidentActionKey),
  CONSTRAINT FK_FactStudentDiscipline_DisciplineIncidentReporterTypeKey FOREIGN KEY (DisciplineIncidentReporterTypeKey) REFERENCES dbo.DimDisciplineIncidentReporterType(DisciplineIncidentReporterTypeKey),
  
  CONSTRAINT FK_FactStudentDiscipline_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);
*/

--discipline -- v2
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentDiscipline' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.FactStudentDiscipline
(
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL, 
  SchoolKey INT NOT NULL,
  DisciplineIncidentKey INT NOT NULL,

  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentDiscipline PRIMARY KEY (StudentKey ASC, TimeKey ASC, SchoolKey ASC, DisciplineIncidentKey ASC),
  CONSTRAINT FK_FactStudentDiscipline_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentDiscipline_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentDiscipline_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey),
  CONSTRAINT FK_FactStudentDiscipline_DisciplineIncidentKey FOREIGN KEY (DisciplineIncidentKey) REFERENCES dbo.DimDisciplineIncident(DisciplineIncidentKey), 
  
  CONSTRAINT FK_FactStudentDiscipline_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);



--assessment
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimAssessment' 
			   AND TABLE_SCHEMA = 'dbo')
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

	CONSTRAINT PK_DimAssessment PRIMARY KEY (AssessmentKey),
    CONSTRAINT FK_DimAssessment_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);


if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentAssessmentScore' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.FactStudentAssessmentScore
(
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
  CONSTRAINT FK_FactStudentAssessmentScore_TimeKey FOREIGN KEY (AssessmentKey) REFERENCES dbo.DimAssessment(AssessmentKey),
  CONSTRAINT FK_FactStudentAssessmentScore_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);

if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'DimCourse' 
			   AND TABLE_SCHEMA = 'dbo')
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

  	CONSTRAINT PK_DimCourse PRIMARY KEY (CourseKey),
    CONSTRAINT FK_DimCourse_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);

if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'FactStudentCourseTranscript' 
			   AND TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.FactStudentCourseTranscript
(
  StudentKey INT NOT NULL,
  TimeKey INT NOT NULL, 
  CourseKey INT NOT NULL,
  SchoolKey INT NOT NULL,
  EarnedCredits INT NOT NULL,
  PossibleCredits INT NOT NULL,
  FinalLetterGradeEarned NVARCHAR(10)  NULL,
  FinalNumericGradeEarned DECIMAL(9,2) NULL,
    
  [LineageKey] INT NOT NULL,

  CONSTRAINT PK_FactStudentCourseTranscript PRIMARY KEY (StudentKey ASC, TimeKey ASC, CourseKey ASC),
  CONSTRAINT FK_FactStudentCourseTranscript_StudentKey FOREIGN KEY (StudentKey) REFERENCES dbo.DimStudent(StudentKey),
  CONSTRAINT FK_FactStudentCourseTranscript_TimeKey FOREIGN KEY (TimeKey) REFERENCES dbo.DimTime(TimeKey),
  CONSTRAINT FK_FactStudentCourseTranscript_CourseKey FOREIGN KEY (CourseKey) REFERENCES dbo.DimCourse(CourseKey) ,
  CONSTRAINT FK_FactStudentCourseTranscript_SchoolKey FOREIGN KEY (SchoolKey) REFERENCES dbo.DimSchool(SchoolKey),
  CONSTRAINT FK_FactStudentCourseTranscript_LineageKey FOREIGN KEY ([LineageKey]) REFERENCES dbo.Lineage([LineageKey])
);



--- Views 
----------------------------------------------------------
--assessment scores

PRINT 'creating view :  View_StudentAssessmentScores'
GO

CREATE VIEW dbo.View_StudentAssessmentScores
AS(
SELECT StudentId, 
       StudentStateId, 
	   FirstName, 
	   LastName, 
	   AssessmentIdentifier, 
	   AssessmentTitle,
	   AssessmentDate,  
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
		SELECT ds.StudentUniqueId AS StudentId,
			   ds.StateId AS StudentStateId,
			   ds.FirstName,
			   ds.LastSurname AS LastName,
			   da.AssessmentIdentifier,
			   da.AssessmentTitle,
			   dt.SchoolDate AS AssessmentDate, 
			   da.[ReportingMethodDescriptor_CodeValue] AS ScoreType,
			   fas.ScoreResult AS Score
		FROM dbo.FactStudentAssessmentScore fas 
			 INNER JOIN dbo.DimStudent ds ON fas.StudentKey = ds.StudentKey
			 INNER JOIN dbo.DimTime dt ON fas.TimeKey = dt.TimeKey	 
			 INNER JOIN dbo.DimAssessment da ON fas.AssessmentKey = da.AssessmentKey		
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
   ) AS PivotTable
);
GO

--attendance by day
PRINT 'creating view :  View_StudentAttendanceByDay'
GO


CREATE VIEW dbo.View_StudentAttendanceByDay
AS(
SELECT StudentId, 
       StudentStateId, 
	   FirstName, 
	   LastName, 
	   SchoolName, 
	   AttedanceDate,

	   --pivoted from row values	  
	   [Early departure],
	   [Excused Absence],
	   [Unexcused Absence],
	   [No Contact],
	   [In Attendance],
	   [Tardy]
	   
FROM (
		SELECT DISTINCT 
		       ds.StudentUniqueId AS StudentId,
			   ds.StateId AS StudentStateId,
			   ds.FirstName,
			   ds.LastSurname AS LastName,
			   dsc.NameOfInstitution AS SchoolName,
			   dt.SchoolDate AS AttedanceDate, 		
			   dact.AttendanceEventCategoryDescriptor_CodeValue AS AttendanceType			   
		FROM dbo.[FactStudentAttendanceByDay] fsabd 
			 INNER JOIN dbo.DimStudent ds ON fsabd.StudentKey = ds.StudentKey
			 INNER JOIN dbo.DimTime dt ON fsabd.TimeKey = dt.TimeKey	 
			 INNER JOIN dbo.DimSchool dsc ON fsabd.SchoolKey = dsc.SchoolKey	 
			 INNER JOIN dbo.DimAttendanceEventCategory dact ON fsabd.AttendanceEventCategoryKey = dact.AttendanceEventCategoryKey		
	    WHERE ds.StudentUniqueId = 363896
		--AND dt.SchoolDate = '2019-09-10'

		
	) AS SourceTable 
PIVOT 
   (
      count(AttendanceType)
	  FOR AttendanceType IN ([Early departure],
							 [Excused Absence],
							 [Unexcused Absence],
							 [No Contact],
							 [In Attendance],
							 [Tardy]
						)
   ) AS PivotTable
);
GO



--behavior incidents
PRINT 'creating view :  View_StudentDiscipline'
GO


CREATE VIEW dbo.View_StudentDiscipline
AS(
SELECT DISTINCT 
		ds.StudentUniqueId AS StudentId,
		ds.StateId AS StudentStateId,
		ds.FirstName,
		ds.LastSurname AS LastName,
		dsc.NameOfInstitution AS SchoolName,
		dt.SchoolDate AS IncidentDate, 		
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

--behavior incidents
PRINT 'creating view :  View_StudentCourseTranscript'
GO


CREATE VIEW dbo.View_StudentCourseTranscript
AS(
SELECT DISTINCT 
		ds.StudentUniqueId AS StudentId,
		ds.StateId AS StudentStateId,
		ds.FirstName,
		ds.LastSurname AS LastName,
		dsc.NameOfInstitution AS SchoolName,
		dc.CourseCode,
		dc.CourseTitle,
		dc.CourseLevelCharacteristicTypeDescriptor_CodeValue AS CourseType,
		dc.SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue AS MassCourseType,
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

PRINT 'creating view :  View_StudentRoster'
GO


CREATE VIEW dbo.View_StudentRoster
AS(
SELECT DISTINCT TOP 1000
		ds.StudentUniqueId AS StudentId,
		ds.StateId AS StudentStateId,
		ds.FirstName,
		ds.MiddleName,
		ds.MiddleInitial,
		ds.FullName,
		ds.LastSurname AS LastName,
		ds.PrimaryElectronicMailAddress AS StudentEmail,
		ds.GradeLevelDescriptor_CodeValue AS GradeLevel,
		ds.BirthDate,
		ds.StudentAge,
		ds.[GraduationSchoolYear],
		dsc.NameOfInstitution AS SchoolName,
		dsc.StateSchoolCode AS SchoolStateCode,
		dsc.UmbrellaSchoolCode AS SchoolUmbrellaCode,
		ds.Homeroom,
		ds.HomeroomTeacher,
		ds.SexType_Code AS Sex,
		ds.[SexType_Male_Indicator],
	    ds.[SexType_Female_Indicator],
	    ds.[SexType_NotSelected_Indicator],


		ds.RaceCode AS Race,
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
		ds.LimitedEnglishProficiencyDescriptor_CodeValue AS LEPCode,
		ds.[LimitedEnglishProficiency_EnglishLearner_Indicator],
		ds.[LimitedEnglishProficiency_Former_Indicator],
		ds.[LimitedEnglishProficiency_NotEnglisLearner_Indicator],
		ds.[EconomicDisadvantage_Indicator],


		ds.ValidFrom,
		ds.ValidTo,
		ds.IsCurrent
		
FROM dbo.DimStudent ds 		
     INNER JOIN dbo.DimSchool dsc ON ds.SchoolKey = dsc.SchoolKey
		
);

GO


--ETL Related Objects
------------------------------------------------------------------------------

--functions
--------------------------------------------------------------
--create function to derive schoolyear from a date

DROP FUNCTION IF EXISTS dbo.Func_GetSchoolYear
GO

CREATE FUNCTION dbo.Func_GetSchoolYear
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

DROP FUNCTION IF EXISTS dbo.Func_GetEasterHolidays
GO
CREATE FUNCTION dbo.Func_GetEasterHolidays
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

DROP FUNCTION IF EXISTS dbo.[Func_GetHolidayFromDate]
GO
CREATE FUNCTION [dbo].[Func_GetHolidayFromDate]
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

DROP FUNCTION IF EXISTS dbo.Func_GetFullName
GO
CREATE FUNCTION dbo.Func_GetFullName
(
    @fName NVARCHAR(256),
    @mName NVARCHAR(256),
    @lName NVARCHAR(256)
)
RETURNS NVARCHAR(768)
AS
BEGIN
    DECLARE @fullName NVARCHAR(768);
    SELECT @fullName
        = LTRIM(RTRIM(LTRIM(@fName)) + RTRIM(' ' + LTRIM(ISNULL(@mName, ''))) + RTRIM(' ' + LTRIM(@lName)));
    RETURN @fullName;
END;

GO
 
 IF exists (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'School' 
			   AND TABLE_SCHEMA = 'Staging')
      DROP TABLE Staging.School; 


--school
if NOT EXISTS (select 1
             FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = 'School' 
			   AND TABLE_SCHEMA = 'Staging')
CREATE TABLE Staging.School
(
  SchoolKey int NOT NULL IDENTITY(1,1), -- ex 9/1/2019 : 20190901 -- surrogate
  [_sourceKey] NVARCHAR(50) NOT NULL,  --'Ed-Fi|Id'
  
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
  ValidTo DATETIME NOT NULL

  CONSTRAINT PK_StagingSchool PRIMARY KEY (SchoolKey),
  
);
GO

IF EXISTS (
        SELECT 1
        FROM sys.procedures WITH(NOLOCK)
        WHERE NAME = 'Proc_ETL_Lineage_GetKey'
		      AND type = 'P'
      )
     DROP PROCEDURE [dbo].[Proc_ETL_Lineage_GetKey]
GO


CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_Lineage_GetKey]
@LoadType nvarchar(1),
@TableName nvarchar(100)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	-- The load for @TableName starts now 
	DECLARE @StartLoad datetime = SYSDATETIME();

	
	INSERT INTO LongitudinalPOC.[dbo].[Lineage](
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
			UPDATE LongitudinalPOC.[dbo].[IncrementalLoads]
			SET LoadDate = '1753-01-01'
			WHERE TableName = @TableName

			DECLARE @sqlCmd NVARCHAR(MAX) = 'TRUNCATE TABLE ' + @TableName;
			EXEC sp_executesql @sqlCmd;
		END;

	-- Select the key of the previously inserted row
	SELECT MAX([LineageKey]) AS LineageKey
	FROM LongitudinalPOC.dbo.[Lineage]
	WHERE 
		[TableName] = @TableName
		AND StartTime = @StartLoad

	RETURN 0;
END;
GO



IF EXISTS (
        SELECT 1
        FROM sys.procedures
        WHERE NAME = 'Proc_ETL_IncrementalLoads_GetLastLoadedDate'
		      AND type = 'P'
      )
     DROP PROCEDURE [dbo].[Proc_ETL_IncrementalLoads_GetLastLoadedDate]
GO


CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_IncrementalLoads_GetLastLoadedDate]
@TableName nvarchar(100)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	-- If the procedure is executed with a wrong table name, throw an error.
	IF NOT EXISTS(SELECT 1 FROM sys.tables WHERE name = @TableName AND Type = N'U')
	BEGIN
        PRINT N'The table does not exist in the data warehouse.';
        THROW 51000, N'The table does not exist in the data warehouse.', 1;
        RETURN -1;
	END
	
    -- If the table exists, but was never loaded before, there won't have a record for it
	-- A record is created for the @TableName with the minimum possible date in the LoadDate column
	IF NOT EXISTS (SELECT 1 FROM LongitudinalPOC.[dbo].[IncrementalLoads] WHERE TableName = @TableName)
		INSERT INTO LongitudinalPOC.[dbo].[IncrementalLoads](TableName,LoadDate)
		SELECT @TableName, '1753-01-01'

    -- Select the LoadDate for the @TableName
	SELECT 
		[LoadDate] AS [LoadDate]
    FROM LongitudinalPOC.[dbo].[IncrementalLoads]
    WHERE 
		TableName = @TableName;

    RETURN 0;
END;
GO

--EXEC SP_ETL_School_PopulateStaging
--EXEC SP_ETL_School_PopulateProduction