SELECT
	CountryKey,
	CompanyKey,
	ProductKey,
	ProductCode,
	ProductName,
	ProductDisplayName,
	DomainID,
	PlanName,
	ProductType,
	ProductGroup,
	PolicyType,
	ProductClassification,
	PlanType,
	TripType,
	FinanceProductCode,
	FinanceProductCode_OLD,
	PlanCode,
	FORMAT(GETDATE(),'yyyyMMdd') as Updated
FROM
	[db-au-workspace].dbo.[vDataMappingProductPlan]
where isnull(isMapped,0) = 0