DELETE FROM [EdFiDW].[Derived].[StudentAssessmentScore];


INSERT INTO [EdFiDW].[Derived].[StudentAssessmentScore]
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
    
SELECT [StudentKey],
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
		SELECT fas.[StudentKey],
               fas.[TimeKey],
               fas.[AssessmentKey],
			   da.[ReportingMethodDescriptor_CodeValue] AS ScoreType,
			   fas.ScoreResult AS Score
		FROM [EdFiDW].dbo.FactStudentAssessmentScore fas  
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
   ) AS PivotTable;
