# Define fields (field names as used by clinical trials api) in clinical trial node type (hub node) 
ClinicalTrial = ['Acronym','BriefSummary','BriefTitle','CompletionDate','CompletionDateType','LastKnownStatus',
    'LastUpdatePostDate','LastUpdatePostDateType','LastUpdateSubmitDate','NCTId','NCTIdAlias','OfficialTitle',
    'OverallStatus','Phase','PrimaryCompletionDate','PrimaryCompletionDateType','ResultsFirstPostDate',
    'ResultsFirstPostDateType','ResultsFirstPostedQCCommentsDate','ResultsFirstPostedQCCommentsDateType',
    'SecondaryId','SecondaryIdDomain','SecondaryIdLink','SecondaryIdType','SeeAlsoLinkLabel','SeeAlsoLinkURL',
    'StartDate','StartDateType','StudyFirstPostDate','StudyFirstPostDateType','StudyType']

# define fields for additional node types (attach to hub node)
IndividualPatientData = ['AvailIPDComment','AvailIPDId','AvailIPDType','AvailIPDURL','IPDSharing',
    'IPDSharingAccessCriteria','IPDSharingDescription','IPDSharingInfoType','IPDSharingTimeFrame','IPDSharingURL']

Sponsor = ['CentralContactEMail','CentralContactName','CentralContactPhone','CentralContactPhoneExt',
    'CentralContactRole','LeadSponsorClass','LeadSponsorName','OrgClass','OrgFullName','OrgStudyId','OrgStudyIdLink',
    'OrgStudyIdType','OverallOfficialAffiliation','OverallOfficialName','OverallOfficialRole','PointOfContactEMail',
    'PointOfContactOrganization','PointOfContactPhone','PointOfContactPhoneExt','PointOfContactTitle',
    'ResponsiblePartyInvestigatorAffiliation','ResponsiblePartyInvestigatorFullName',
    'ResponsiblePartyInvestigatorTitle','ResponsiblePartyOldNameTitle','ResponsiblePartyOldOrganization',
    'ResponsiblePartyType']

Collaborator = ['CollaboratorClass','CollaboratorName']

Condition = ['Condition','ConditionAncestorId','ConditionAncestorTerm','ConditionBrowseBranchAbbrev',
    'ConditionBrowseBranchName','ConditionBrowseLeafAsFound','ConditionBrowseLeafId','ConditionBrowseLeafName',
    'ConditionBrowseLeafRelevance','ConditionMeshId','ConditionMeshTerm']

StudyDesign = ['DesignAllocation','DesignInterventionModel','DesignInterventionModelDescription','DesignMasking',
    'DesignMaskingDescription','DesignObservationalModel','DesignPrimaryPurpose','DesignTimePerspective',
    'DetailedDescription','PrimaryOutcomeDescription','PrimaryOutcomeMeasure','PrimaryOutcomeTimeFrame',
    'SamplingMethod']

Participant = ['EligibilityCriteria','EnrollmentCount','EnrollmentType','Gender','GenderBased','GenderDescription',
    'HealthyVolunteers','MaximumAge','MinimumAge','StdAge','StudyPopulation']

ExpandedAccess = ['ExpAccTypeIndividual','ExpAccTypeIntermediate','ExpAccTypeTreatment','ExpandedAccessNCTId',
    'ExpandedAccessStatusForNCTId','HasExpandedAccess']

Intervention = ['InterventionBrowseLeafId','InterventionBrowseLeafName','InterventionBrowseLeafRelevance',
    'InterventionDescription','InterventionMeshId','InterventionMeshTerm','InterventionName','InterventionOtherName',
    'InterventionType','IsFDARegulatedDevice','IsFDARegulatedDrug']

Location = ['LocationCity','LocationContactEMail','LocationContactName','LocationContactPhone',
    'LocationContactPhoneExt','LocationContactRole','LocationCountry','LocationFacility','LocationState',
    'LocationStatus','LocationZip']

PatientRegistry = ['PatientRegistry']

Reference = ['ReferenceCitation','ReferencePMID','ReferenceType','RetractionPMID','RetractionSource']

# list of lists of fields for each additional node type
additional_class_fields = [IndividualPatientData, Sponsor, Collaborator, Condition, StudyDesign, Participant, 
    ExpandedAccess, Intervention, Location, PatientRegistry, Reference]

# name for each additional node type
additional_class_names = ['IndividualPatientData','Sponsor','Collaborator','Condition','StudyDesign',
    'Participant','ExpandedAccess','Intervention','Location','PatientRegistry','Reference']

# name for connection from hub node to additional node type
additional_class_connections = ['has_individual_patient_data','sponsored_by','collaborated_with','includes_conditions',
    'has_study_design','has_participant_info','expanded_access_info','uses_interventions','in_locations',
    'patient_registry_info','has_references']

# variable name to use in cypher
additional_class_variable_names = ['ind','spo','col','con','stu','par','exp','int','loc','pat','ref']

# list of field types that may not use strings: 1D or 2D array. These fields are handled differently
special_types = {'OtherEventStatsGroupId', 'OtherEventStatsNumAffected', 'OtherEventStatsNumAtRisk',
    'OutcomeMeasurementGroupId', 'OutcomeMeasurementValue', 'SeriousEventStatsGroupId', 'SeriousEventStatsNumAffected',
    'SeriousEventStatsNumAtRisk', 'OtherEventStatsNumEvents', 'OutcomeClassDenomCountGroupId',
    'OutcomeClassDenomCountValue', 'SeriousEventStatsNumEvents', 'OutcomeMeasurementComment', 'OutcomeClassDenomUnits',
    'OutcomeMeasurementSpread', 'OutcomeClassTitle', 'OutcomeMeasurementLowerLimit', 'OutcomeMeasurementUpperLimit',
    'LocationContactName', 'LocationContactRole', 'SeriousEventAssessmentType', 'SeriousEventOrganSystem',
    'SeriousEventSourceVocabulary', 'SeriousEventTerm', 'LocationCity', 'LocationCountry', 'LocationFacility',
    'LocationZip', 'LocationStatus', 'LocationState', 'OutcomeAnalysisGroupId', 'OtherEventAssessmentType',
    'OtherEventOrganSystem', 'OtherEventSourceVocabulary', 'OtherEventTerm', 'LocationContactPhone',
    'BaselineMeasurementGroupId', 'BaselineMeasurementValue', 'LocationContactEMail', 'OutcomeDenomCountGroupId',
    'OutcomeDenomCountValue', 'OutcomeGroupId', 'OutcomeGroupTitle', 'OutcomeGroupDescription', 'OutcomeCategoryTitle',
    'OutcomeAnalysisGroupDescription', 'OutcomeAnalysisNonInferiorityType', 'OutcomeAnalysisPValue',
    'OutcomeAnalysisStatisticalMethod', 'OutcomeAnalysisCILowerLimit', 'OutcomeAnalysisCINumSides',
    'OutcomeAnalysisCIPctValue', 'OutcomeAnalysisCIUpperLimit', 'OutcomeAnalysisParamType',
    'OutcomeAnalysisParamValue', 'OutcomeAnalysisPValueComment', 'OutcomeAnalysisEstimateComment', 'FlowReasonGroupId',
    'FlowReasonNumSubjects', 'BaselineClassDenomCountGroupId', 'BaselineClassDenomCountValue',
    'FlowAchievementGroupId', 'FlowAchievementNumSubjects', 'OutcomeAnalysisNonInferiorityComment',
    'ConditionBrowseLeafId', 'ConditionBrowseLeafName', 'ConditionBrowseLeafRelevance', 'Keyword',
    'OutcomeAnalysisStatisticalComment', 'SecondaryOutcomeDescription', 'SecondaryOutcomeMeasure',
    'SecondaryOutcomeTimeFrame', 'InterventionOtherName', 'OutcomeAnalysisDispersionType',
    'OutcomeAnalysisDispersionValue', 'OtherEventNotes', 'OutcomeAnalysisTestedNonInferiority', 'ReferenceCitation',
    'ReferenceType', 'Condition', 'SeriousEventNotes', 'ArmGroupInterventionName', 'ConditionBrowseLeafAsFound',
    'InterventionArmGroupLabel', 'ReferencePMID', 'OutcomeDenomUnits', 'OutcomeMeasureDescription',
    'OutcomeMeasureParamType', 'OutcomeMeasurePopulationDescription', 'OutcomeMeasureReportingStatus',
    'OutcomeMeasureTimeFrame', 'OutcomeMeasureTitle', 'OutcomeMeasureType', 'OutcomeMeasureUnitOfMeasure',
    'FlowAchievementNumUnits', 'ConditionAncestorId', 'ConditionAncestorTerm', 'PrimaryOutcomeDescription',
    'PrimaryOutcomeMeasure', 'PrimaryOutcomeTimeFrame', 'BaselineCategoryTitle', 'BaselineClassDenomUnits',
    'OutcomeMeasureDispersionType', 'BaselineClassTitle', 'InterventionBrowseLeafId', 'InterventionBrowseLeafName',
    'InterventionBrowseLeafRelevance', 'BaselineMeasurementSpread', 'OverallOfficialAffiliation',
    'OverallOfficialName', 'OverallOfficialRole', 'FlowAchievementComment', 'ConditionMeshId', 'ConditionMeshTerm',
    'InterventionBrowseLeafAsFound', 'InterventionMeshId', 'InterventionMeshTerm', 'OtherOutcomeDescription',
    'OtherOutcomeMeasure', 'OtherOutcomeTimeFrame', 'InterventionAncestorId', 'InterventionAncestorTerm',
    'BaselineMeasureDenomCountGroupId', 'BaselineMeasureDenomCountValue', 'CollaboratorClass', 'CollaboratorName',
    'BaselineMeasurementLowerLimit', 'BaselineMeasurementUpperLimit', 'FlowMilestoneType',
    'BaselineMeasurementComment', 'SecondaryId', 'SecondaryIdDomain', 'SecondaryIdType',
    'OutcomeMeasureDenomUnitsSelected', 'BaselineMeasureParamType', 'BaselineMeasureTitle',
    'BaselineMeasureUnitOfMeasure', 'BaselineMeasurePopulationDescription', 'BaselineMeasureDescription',
    'InterventionDescription', 'InterventionName', 'InterventionType', 'OutcomeMeasureTypeUnitsAnalyzed',
    'LocationContactPhoneExt', 'BaselineMeasureDispersionType', 'RemovedCountry', 'FlowDropWithdrawType',
    'OutcomeAnalysisOtherAnalysisDescription', 'SecondaryIdLink', 'EventGroupDeathsNumAffected',
    'EventGroupDeathsNumAtRisk', 'EventGroupDescription', 'EventGroupId', 'EventGroupOtherNumAffected',
    'EventGroupOtherNumAtRisk', 'EventGroupSeriousNumAffected', 'EventGroupSeriousNumAtRisk', 'EventGroupTitle',
    'ArmGroupDescription', 'ArmGroupLabel', 'ArmGroupType', 'OutcomeMeasureAnticipatedPostingDate',
    'BaselineMeasureDenomUnits', 'SeeAlsoLinkLabel', 'SeeAlsoLinkURL', 'BaselineDenomCountGroupId',
    'BaselineDenomCountValue', 'BaselineGroupDescription', 'BaselineGroupId', 'BaselineGroupTitle', 'FlowPeriodTitle',
    'FlowGroupDescription', 'FlowGroupId', 'FlowGroupTitle', 'UnpostedEventDate', 'UnpostedEventType',
    'InterventionBrowseBranchAbbrev', 'InterventionBrowseBranchName', 'OutcomeMeasureCalculatePct',
    'ConditionBrowseBranchAbbrev', 'ConditionBrowseBranchName', 'BaselineMeasureDenomUnitsSelected',
    'SubmissionReleaseDate', 'AvailIPDId', 'AvailIPDType', 'AvailIPDURL', 'SubmissionResetDate', 'LargeDocDate',
    'LargeDocFilename', 'LargeDocHasICF', 'LargeDocHasProtocol', 'LargeDocHasSAP', 'LargeDocLabel',
    'LargeDocTypeAbbrev', 'LargeDocUploadDate', 'BaselineMeasureCalculatePct', 'FlowMilestoneComment',
    'OutcomeAnalysisCIUpperLimitComment', 'AvailIPDComment', 'SubmissionMCPReleaseN', 'SubmissionUnreleaseDate',
    'IPDSharingInfoType', 'NCTIdAlias', 'DesignWhoMasked', 'RetractionPMID', 'RetractionSource', 'StdAge',
    'BaselineDenomUnits', 'CentralContactEMail', 'CentralContactName', 'CentralContactPhone', 'CentralContactPhoneExt',
    'CentralContactRole', 'Phase'}

