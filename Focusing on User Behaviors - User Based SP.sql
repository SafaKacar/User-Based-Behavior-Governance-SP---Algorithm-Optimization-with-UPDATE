DECLARE @UserAttributesSearchStartDateTime  AS DATETIME 
	    SELECT @UserAttributesSearchStartDateTime  = MAX([RegisterDate]) FROM [DWH_Workspace].[DB\skacar].FACT_UserRetentions With (Nolock)
INSERT INTO [DWH_Workspace].[DB\skacar].FACT_UserRetentions
SELECT
	 u.User_Key [UserKey]
	,CAST(MAX([LastUserType]) AS TINYINT) [LastUserType]
	,CAST(0 AS BIT)					  [IsClosedAccount] --Operand data type bit is invalid for max operator!!!
	,CAST(0 AS BIT)					  [IsPermanentLockOut]
	,MAX(u.CreateDate)				  [RegisterDate]
	,CAST(NULL AS DATETIME)			  [LastLoginDate]
	,CAST(NULL AS INT)				  [LoginCount]
	,CAST(0 AS BIT)					  [IsLastLoginUnsuccessful]
	,CAST(0 AS BIT)					  [HasTransactionsedTransaction]
	,CAST(NULL AS INT)				  [TxCount]
	,CAST(0 AS BIT)					  [HasClosedLoopP2P]
	,CAST(NULL AS DATETIME)			  [LastUserWalletModifiedAt]
	,CAST(NULL AS INT)				  [MaxUserWalletEver]
	,CAST(NULL AS DATETIME)			  [MaxUserWalletCreateDate] 
	,CAST(0 AS BIT)					  [IsLastLoginNewerThanLastTransactionsModified]
	,CAST(NULL AS INT)				  [TenureBetweenRegisterDateAndLoginDate]
	,CAST(NULL AS INT)				  [TenureBetweenRegisterDateLastUserWalletModifiedAt]
	,CAST(NULL AS INT)				  [TenureBetweenRegisterDateLastMaxBalanceModifiedAt]
	,CAST(NULL AS INT)				  [TenureBetweenLastUserWalletModifiedAtAndLastLoginDate]
FROM [DWH_DB].[dbo].[DIM_UserAttributes] u With (Nolock) 
WHERE u.CreateDate >= @UserAttributesSearchStartDateTime
GROUP BY u.User_Key
GO

UPDATE UR
   SET UR.LastUserType = CAST(U.LastUserType AS TINYINT)
	  ,UR.IsClosedAccount	 = CAST(U.IsClosedAccount AS BIT)
	  ,UR.IsPermanentLockOut = CAST(IIF((YEAR(u.PasivenessEndDateUtc)>3000),1,0) AS BIT)
FROM [DWH_DB].[dbo].[DIM_UserAttributes] U with (Nolock)
JOIN [DWH_Workspace].[DB\skacar].FACT_UserRetentions UR WITH (NOLOCK)
ON	 UR.UserKey = U.User_Key
GO
;WITH TransactionsTx_CTE AS
(
	SELECT DISTINCT
		   l1.UserKey
		  ,MAX(AvailableBalance)									MaxAvailableBalance
		  ,MAX(l1.CreateDate)										LastCreateDate
		  ,COUNT(l1.Id)												TxCount
		  ,CAST(IIF(COUNT(OtherUserKey)  > 0, 1, 0) AS BIT)			HasClosedLoopP2P
		  ,MAX(MaxResultedBalanceCreateDate)							MaxResultedBalanceCreateDate
	FROM [DWH_DB].[dbo].[FACT_Transactions] l1 With (Nolock)
	JOIN (
			select UserKey,MAX(CASE WHEN AvailableBalanceRank = 1 THEN CreateDate END) MaxResultedBalanceCreateDate
					from (
							select ls.UserKey,CreateDate
								  ,ROW_NUMBER() OVER (PARTITION BY ls.UserKey ORDER BY AvailableBalance DESC) AvailableBalanceRank
							from [DWH_DB].[dbo].[FACT_Transactions] ls with (Nolock)
							join [DWH_Workspace].[DB\skacar].FACT_UserRetentions urs With (Nolock)  on urs.UserKey = ls.UserKey AND (ls.CreateDate > urs.LastUserWalletModifiedAt OR (ls.CreateDate IS NOT NULL AND urs.LastUserWalletModifiedAt IS NULL))
							where IsCancellation = 0
						  ) z1
					group by UserKey
		 ) l2
	ON l1.UserKey = L2.UserKey
	JOIN [DWH_Workspace].[DB\skacar].FACT_UserRetentions urs With (Nolock) ON URS.UserKey = L1.UserKey AND (l1.CreateDate > urs.LastUserWalletModifiedAt OR (l1.CreateDate IS NOT NULL AND urs.LastUserWalletModifiedAt IS NULL))
	WHERE IsCancellation = 0 and (l1.CreateDate > urs.LastUserWalletModifiedAt OR (l1.CreateDate IS NOT NULL AND urs.LastUserWalletModifiedAt IS NULL))
	GROUP BY l1.UserKey
)
UPDATE UR
   SET UR.HasTransactionsedTransaction = 1
	  ,UR.TxCount = ISNULL(UR.TxCount,0) + LCTE.TxCount
	  ,UR.HasClosedLoopP2P = CASE WHEN UR.HasClosedLoopP2P = 1									 THEN 1
								  WHEN UR.HasClosedLoopP2P = 0 AND LCTE.HasClosedLoopP2P = 1	 THEN 1 
																								 ELSE 0 END
	  ,UR.LastUserWalletModifiedAt = LastCreateDate
	  ,UR.MaxUserWalletEver = CASE WHEN LCTE.MaxAvailableBalance > UR.MaxUserWalletEver						THEN LCTE.MaxAvailableBalance
									WHEN LCTE.MaxAvailableBalance IS NOT NULL AND UR.MaxUserWalletEver IS NULL THEN LCTE.MaxAvailableBalance
																												ELSE UR.MaxUserWalletEver		 END
	  ,UR.MaxUserWalletCreateDate = CASE WHEN LCTE.MaxAvailableBalance > UR.MaxUserWalletEver						 THEN LCTE.MaxResultedBalanceCreateDate
										 WHEN LCTE.MaxAvailableBalance IS NOT NULL AND UR.MaxUserWalletEver IS NULL THEN LCTE.MaxResultedBalanceCreateDate
																													 ELSE UR.MaxUserWalletCreateDate END

FROM [DWH_Workspace].[DB\skacar].FACT_UserRetentions UR WITH (NOLOCK)
JOIN TransactionsTx_CTE LCTE WITH (NOLOCK) ON UR.UserKey = LCTE.UserKey

GO

DECLARE @LoginSearchStartDateTime  AS DATETIME
	    SELECT @LoginSearchStartDateTime  = MAX([LastLoginDate]) FROM [DWH_Workspace].[DB\skacar].FACT_UserRetentions With (Nolock)
;WITH UserLogins_CTE AS
(
	SELECT DISTINCT
		   UserKey
		  ,MAX(CASE WHEN UL.LoginStatus IN (0,2) THEN UL.CreateDate ELSE NULL END) OVER (PARTITION BY UserKey) LastLoginDate
		  ,COUNT(CASE WHEN UL.LoginStatus = 0	 THEN ul.Id		   ELSE NULL END) OVER (PARTITION BY UserKey) LoginCount
	      ,CAST(IIF(((UL1.LastUnsuccessfulCreateDate > UL1.LastSuccessfulCreateDate) OR (UL1.LastUnsuccessfulCreateDate IS NOT NULL AND UL1.LastSuccessfulCreateDate IS NULL)),1,0) AS BIT) IsLastLoginUnsuccessful
	FROM [DWH_DB].[dbo].[FACT_UserLogins] UL with (Nolock)
	JOIN (select UserKey,LastLoginDate from [DWH_Workspace].[DB\skacar].FACT_UserRetentions With (Nolock)) UR on UL.User_Key = UR.UserKey AND (UR.LastLoginDate < UL.CreateDate OR (UR.LastLoginDate IS NULL AND UL.CreateDate IS NOT NULL))
	JOIN (select distinct User_Key,MAX(CASE WHEN LoginStatus = 2 THEN CreateDate ELSE NULL END) OVER (PARTITION BY User_Key) LastUnsuccessfulCreateDate
								  ,MAX(CASE WHEN LoginStatus = 0 THEN CreateDate ELSE NULL END) OVER (PARTITION BY User_Key) LastSuccessfulCreateDate
		  from [DWH_DB].[dbo].[FACT_UserLogins] with(Nolock) where CreateDate>=@LoginSearchStartDateTime) UL1 ON UL1.User_Key = UL.User_Key
	WHERE (UR.LastLoginDate < UL.CreateDate OR (UR.LastLoginDate IS NULL AND UL.CreateDate IS NOT NULL))
)
UPDATE UR
   SET UR.LastLoginDate = CASE WHEN ULCTE.LastLoginDate > UR.LastLoginDate						 THEN ULCTE.LastLoginDate
							   WHEN ULCTE.LastLoginDate IS NOT NULL AND UR.LastLoginDate IS NULL THEN ULCTE.LastLoginDate
																								 ELSE UR.LastLoginDate END
	  ,UR.LoginCount = ISNULL(UR.LoginCount,0) + ULCTE.LoginCount
	  ,UR.IsLastLoginUnsuccessful = ULCTE.IsLastLoginUnsuccessful
	  ,UR.IsLastLoginNewerThanLastTransactionsModified = CAST(IIF(SIGN(DATEDIFF(DAY,UR.LastUserWalletModifiedAt,ULCTE.LastLoginDate))=1,1,0) AS BIT)
	  ,UR.TenureBetweenRegisterDateAndLoginDate  = DATEDIFF(DAY,UR.RegisterDate,ULCTE.LastLoginDate)
	  ,UR.TenureBetweenLastUserWalletModifiedAtAndLastLoginDate = DATEDIFF(DAY,UR.LastUserWalletModifiedAt,ULCTE.LastLoginDate)
	  ,UR.TenureBetweenRegisterDateLastUserWalletModifiedAt	 = DATEDIFF(DAY,UR.RegisterDate,UR.LastUserWalletModifiedAt)
	  ,UR.TenureBetweenRegisterDateLastMaxBalanceModifiedAt		 = DATEDIFF(DAY,UR.RegisterDate,UR.MaxUserWalletCreateDate)
FROM [DWH_Workspace].[DB\skacar].FACT_UserRetentions UR WITH (NOLOCK)
JOIN UserLogins_CTE ULCTE WITH (NOLOCK) ON UR.UserKey = ULCTE.UserKey

/*TABLO BASIM SORGUSU
DECLARE @UserCreatedIntervalStart as Date = '2022-09-01'--çalıştır-yeni
	   ,@UserCreatedIntervalEnd	  as Date = '2022-10-01'
;
WITH CTE_ABC AS
	(
	SELECT DISTINCT
	    U.User_Key		  UserKey
	   ,MAX(U.CreateDate)												  OVER (PARTITION BY u.User_Key)  RegisterDate
	   ,MAX(CASE WHEN UL.LoginStatus IN (0,2) THEN UL.CreateDate ELSE NULL END) OVER (PARTITION BY u.User_Key)  LastLoginDate
	   ,MAX(U.LastUserType)													  OVER (PARTITION BY u.User_Key)  LastUserType
	   ,MAX(CAST(U.IsClosedAccount AS INT))							  OVER (PARTITION BY u.User_Key)  IsClosedAccount
	   ,IIF((YEAR(u.PasivenessEndDateUtc)>3000),1,0)  IsPermanentLockOut
	   ,COUNT(CASE WHEN UL.LoginStatus = 0 THEN ul.Id ELSE NULL END)	  OVER (PARTITION BY u.User_Key)  LoginCount
	   ,IIF(UL1.LastUnsuccessfulCreateDate > UL1.LastSuccessfulCreateDate,1,0) LastLoginTrialisUnsuccessful
	FROM	  [DWH_DB]..[FACT_UserLogins]   UL  with(Nolock) 
		 JOIN [DWH_DB]..[DIM_UserAttributes]		    U   with(Nolock) ON  UL.User_Key =  U.User_Key AND U.CreateDate >= @UserCreatedIntervalStart AND U.CreateDate < @UserCreatedIntervalEnd
	LEFT JOIN (select distinct User_Key,MAX(CASE WHEN LoginStatus = 2 THEN CreateDate ELSE NULL END) OVER (PARTITION BY User_Key) LastUnsuccessfulCreateDate
									   ,MAX(CASE WHEN LoginStatus = 0 THEN CreateDate ELSE NULL END) OVER (PARTITION BY User_Key) LastSuccessfulCreateDate
			   from [DWH_DB]..[FACT_UserLogins] with(Nolock) where CreateDate>=@UserCreatedIntervalStart) UL1 ON UL1.User_Key = UL.User_Key AND UL1.LastUnsuccessfulCreateDate > UL1.LastSuccessfulCreateDate
			   where UL.CreateDate >= @UserCreatedIntervalStart
	), A AS
	(
	 SELECT DISTINCT
					  UnionedTransactions.Id
					 ,UnionedTransactions.UserKey
					 ,UnionedTransactions.OtherUserKey
					 ,UnionedTransactions.FeatureType
					 ,UnionedTransactions.AvailableBalance
					 ,UnionedTransactions.CreateDate
					 ,COUNT(UnionedTransactions.Id)	   OVER (PARTITION BY UserKey)								  TxCount
					 ,ROW_NUMBER() OVER (PARTITION BY UserKey ORDER BY UnionedTransactions.CreateDate DESC) RankForLastAvailableBalanceDateTime
					 ,ROW_NUMBER() OVER (PARTITION BY UserKey ORDER BY AvailableBalance		   DESC) RankForLastMaxAvailableBalance
					 ,DENSE_RANK() OVER (PARTITION BY UserKey ORDER BY FeatureType			) RankForDistinctFeatureType
					 ,DENSE_RANK() OVER (PARTITION BY (CASE WHEN OtherUserKey IS NOT NULL THEN UserKey END) ORDER BY OtherUserKey) RankForDistinctOtherUserKey
			  FROM (
					select Id,CreateDate,UserKey,FeatureType,AvailableBalance,OtherUserKey from [DWH_DB]..[FACT_Transactions]		  with(Nolock) where IsCancellation = 0 and CreateDate >= @UserCreatedIntervalStart
				 --    union
					--select Id,CreateDate,UserKey,FeatureType,AvailableBalance,OtherUserKey from [Transactions2020Before]..[FACT_Transactions] with(Nolock) where IsCancellation = 0 and CreateDate >= @UserCreatedIntervalStart
				   ) UnionedTransactions
			  JOIN DIM_UserAttributes U with (nolock) ON UnionedTransactions.UserKey = U.User_Key and U.CreateDate >= @UserCreatedIntervalStart AND U.CreateDate < @UserCreatedIntervalEnd
	),B AS
	(
	 SELECT
	   A.UserKey
	  ,TxCount
	  ,MAX(CASE WHEN RankForLastAvailableBalanceDateTime = 1 THEN CreateDate		   ELSE NULL END) OVER (PARTITION BY UserKey) LastUserWalletModifiedAt
	  ,MAX(CASE WHEN RankForLastMaxAvailableBalance		 = 1 THEN AvailableBalance ELSE NULL END) OVER (PARTITION BY UserKey) MaxUserWalletEver
	  ,MAX(CASE WHEN RankForLastMaxAvailableBalance		 = 1 THEN CreateDate		   ELSE NULL END) OVER (PARTITION BY UserKey) MaxUserWalletCreateDate
	  ,MAX(			 RankForDistinctFeatureType)													  OVER (PARTITION BY UserKey) DistinctFeatureType
	  ,MAX(			 RankForDistinctOtherUserKey)												  OVER (PARTITION BY UserKey) DistinctOtherUserKey
	FROM A
	)
	INSERT INTO DWH_Workspace..LastLoginAnalysis
	SELECT distinct
			 CTE_ABC.UserKey
			,CTE_ABC.LastUserType
			,CTE_ABC.IsClosedAccount
			,CTE_ABC.IsPermanentLockOut
			,CTE_ABC.RegisterDate
			,CTE_ABC.LastLoginDate
			,CTE_ABC.LastLoginTrialisUnsuccessful
			,IIF(B.TxCount IS NULL,0,1) HasTransactionsedTransaction
			,B.DistinctFeatureType
			,B.TxCount
			,IIF(COUNT(DistinctOtherUserKey) over (partition by B.UserKey)=0,0,1) HasClosedLoopP2P
			,B.DistinctOtherUserKey
			,B.LastUserWalletModifiedAt
			,B.MaxUserWalletEver
			,B.MaxUserWalletCreateDate
			,IIF(SIGN(DATEDIFF(DAY,B.LastUserWalletModifiedAt,CTE_ABC.LastLoginDate))=1,1,0) BiggerLastLoginTenureThanLastUserWalletModifiedAtTenure
			,DATEDIFF(DAY,CTE_ABC.RegisterDate,CTE_ABC.LastLoginDate)		TenureBetweenRegisterDateAndLoginDate
			,DATEDIFF(DAY,CTE_ABC.RegisterDate,B.LastUserWalletModifiedAt) TenureBetweenRegisterDateLastUserWalletModifiedAt
			,DATEDIFF(DAY,CTE_ABC.RegisterDate,B.MaxUserWalletCreateDate)	TenureBetweenRegisterDateLastMaxBalanceModifiedAt -- En yüksek tutarına ne kadar günlük kullanıcıyken yaptı
			,DATEDIFF(DAY,B.LastUserWalletModifiedAt,CTE_ABC.LastLoginDate)TenureBetweenLastUserWalletModifiedAtAndLastLoginDate
	--  INTO DWH_Workspace..LastLoginAnalysis
	  FROM CTE_ABC
	  LEFT JOIN B ON CTE_ABC.UserKey = B.UserKey
*/