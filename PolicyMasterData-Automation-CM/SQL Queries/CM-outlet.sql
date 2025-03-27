-- completed prefilled query for ULDWH02
SELECT
        o.Country,
        d.DomainID,
        penOut.SuperGroupName, 
        o.GroupCode,
        o.GroupName,
        o.SubGroupCode,
        o.SubGroupName,
        o.AlphaCode,o.OutletName,
        o.Distributor,
        o.JV,
        case 
            when o.Channel ='Retail' then 70
            when o.Channel ='Call Centre'then 71
            when o.Channel ='Website White-Label'then 72
            when o.Channel ='Integrated'then 73
            when o.Channel ='Mobile'then 74
            when o.Channel ='Point of Sale'then 75
            else null
        end as Channel,
        o.JVDesc,
		FORMAT(GETDATE(),'yyyyMMdd') as UpdateDate
    from [db-au-star].dbo.dimOutlet o with(nolock)
    outer apply(
        select top 1 DomainID
        from [db-au-cmdwh].dbo.penDomain with(nolock)
        where CountryKey = o.Country
    ) d 
	outer apply( -- this retrieves a new SuperGroupName corresponding to past pairings of GroupCode and SuperGroupName
		select top 1 SuperGroupName
		FROM [db-au-cmdwh].dbo.penOutlet
		WHERE GroupCode = o.GroupCode
		ORDER BY outletStartDate DESC -- retrieve most recent

	) penOut
    where o.isLatest ='Y'
    and OutletKey <>''
    and o.JV <>'Unknown'
    and o.Country <>'UNKNOWN'
    and (o.SuperGroupName ='') -- this removes outlets with an existing SuperGroupName
    order by o.Country,o.GroupName,o.SubGroupName,o.AlphaCode
