SELECT
	OutletAlphaKey,
	CountryKey,
	DomainID,
	SuperGroupName,
	GroupCode,
	GroupName,
	SubGroupCode,
	SubGroupName,
	AlphaCode,
	OutletName,
	DistributionType,
	JVCode,
	Channel,
	CONVERT(VARCHAR(33), CommencementDate, 126) AS CommencementDate,
	TradingStatus,
	LastModify,
	FirstSell,
	LastSell
FROM [db-au-workspace].dbo.vpenOutletUnmappedJV
where GroupCode='CF'