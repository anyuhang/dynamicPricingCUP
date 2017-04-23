library(data.table)
semrush = data.table(read.csv('/Users/yan/Code/semrush/semRushResult.csv', header=T,sep=','))
dp_census = data.table(read.csv('/Users/yan/Code/semrush/dp_census.csv', header=T,sep=','))
dp_sem_mkt = data.table(read.csv('/Users/yan/Code/semrush/dp_sem_mkt.csv', header=T,sep=','))
dp_mkt_geo = data.table(read.csv('/Users/yan/Code/semrush/dp_mkt_geo.csv', header=T,sep=','))
semrush_sum=semrush[CPC>0,.(totalCPC = sum(search.volume*CPC), totalVol=sum(search.volume)), by=ad_market_id]
semrush_sum[,avgCPC:=totalCPC/totalVol]
raw=merge(raw, semrush_sum[,.(ad_market_id,totalVol, avgCPC)], by.x='ad_market_id', by.y='ad_market_id', all.x=T)
raw[, `:=`(hhincome=as.numeric(as.character(hhincome)), nocollegepct=as.numeric(as.character(nocollegepct)),
             loghhincome=log(as.numeric(as.character(hhincome))), loghh=log(households))]
raw[, `:=`(ndivoiced=(divoiced-mean(divoiced))/sd(divoiced),
npoverty=(poverty-mean(poverty))/sd(poverty), nhispanicpct=(hispanicpct-mean(hispanicpct))/sd(hispanicpct),
nnocollegepct=(nocollegepct-mean(nocollegepct))/sd(nocollegepct),
nloghhincome=(loghhincome-mean(loghhincome))/sd(loghhincome), nloghh=(loghh-mean(loghh))/sd(loghh) )]

#### build model and predict CPC
cpc_pred = data.frame(ad_region=character(),ad_specialty=character(),avgCPC=numeric(),pred=numeric())
##ad_specialty_list = unique(cpc_tmp[lm_ind==1,.(len=length(ad_region)),by=ad_specialty][len>5,.(ad_specialty)])
ad_specialty_list = c('DUI & DWI','Family','Immigration','Divorce & Separation',
  'Criminal Defense','Personal Injury','Car Accidents','Bankruptcy & Debt',
  'Workers Compensation','Probate','Employment & Labor')
for (i in ad_specialty_list) {
    training_data = raw[ad_specialty==i & avgCPC>0,]
    if ( length(training_data)>=10 )  {
        scoring_data = raw[ad_specialty==i,]
        mylm = lm(avgCPC ~ nloghh+nloghhincome+ndivoiced+npoverty+nhispanicpct+nnocollegepct, data=training_data)
        summary(mylm)
        scoring_data$pred = predict(mylm,scoring_data[,.(nloghh,nloghhincome,ndivoiced,npoverty,nhispanicpct,nnocollegepct)])
        cpc_pred = rbind(cpc_pred, scoring_data[,.(ad_region,ad_specialty,avgCPC,pred)]) }
}

mrr = data.table(read.csv('/Users/yan/Code/semrush/mrr_customer.csv', header=T,sep=','))
churn_risk = data.table(read.csv('/Users/yan/Code/semrush/ChurnScore201703_Q_2017-03-09.csv', header=T,sep=','))
mrr=merge(mrr, churn_risk[,.(customer_id,ChurnProb)],by.x='customer_id',by.y='customer_id',all.x=T)
mrr_agg=mrr[,.(mrr=sum(mrr_current_month), churnRisk=sum(mrr_current_month*ChurnProb)), by=market_id]
raw=merge(raw, mrr_agg[,.(market_id, mrr, churnRisk, churnRiskRatio=churnRisk/mrr)],
by.x='ad_market_id', by.y='market_id', all.x=T)
raw=merge(raw, cpc_pred[,.(ad_region,ad_specialty,pred)],
by.x=c('ad_region','ad_specialty'), by.y=c('ad_region','ad_specialty'), all.x=T)

raw[,rush_cpc_potential:=ifelse(pred/cup>=1.5, 0.3, ifelse(pred/cup>=1.3, 0.2, ifelse(pred/cup>=1.1, 0.1,0)))]
raw[,churn_potential:=ifelse(churnrate>=0.05,0,ifelse(churnrate>=0.03,0.1,ifelse(churnrate>=0.01,0.2, 0.3)))]
raw[,sell_through_potential:=ifelse(sl_sell_through>=1.5,0.3,ifelse(sl_sell_through>=1.25,0.2,ifelse(sl_sell_through>=0.8,0.1,0)))]
raw[,churn_risk_potential:=ifelse(churnRiskRatio>=0.06,0,ifelse(churnRiskRatio>=0.03,0.1,ifelse(churnRiskRatio>=0.01,0.2,0.3)))]
raw[,lawyer_potential:=ifelse(lawyers>=30,0.3,ifelse(lawyers>=20,0.2,ifelse(lawyers>=10,0.1,0)))]
raw[,cup_potential:=(rush_cpc_potential+churn_potential+sell_through_potential+churn_risk_potential+lawyer_potential)/5]

write.table(raw, file="/Users/yan/Code/semrush/cup_potential.csv",row.names=FALSE,sep=",")

