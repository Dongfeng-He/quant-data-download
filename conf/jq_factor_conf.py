# coding: utf-8

# 调试
# 未实现的 alpha101 因子
UNIMPLEMENTED_ALPHA101_LIST = [48, 58, 59, 63, 67, 69, 70, 76, 79, 80, 81, 82, 87, 89, 90, 91, 93, 97, 100]
# 未实现的 alpha191 因子
UNIMPLEMENTED_ALPHA191_LIST = [30, 143, 165, 183]

FACTOR_NAME_LIST = [
    "administration_expense_ttm",		# 管理费用TTM	基础科目及衍生类因子
    "asset_impairment_loss_ttm",		# 资产减值损失TTM	基础科目及衍生类因子
    "cash_flow_to_price_ratio",		    # 现金流市值比	基础科目及衍生类因子
    "circulating_market_cap",		    # 流通市值	基础科目及衍生类因子
    "EBIT",		                        # 息税前利润	基础科目及衍生类因子
    "EBITDA",		                    # 息税折旧摊销前利润	基础科目及衍生类因子
    "financial_assets",		            # 金融资产	基础科目及衍生类因子
    "financial_expense_ttm",		    # 财务费用TTM	基础科目及衍生类因子
    "financial_liability",		        # 金融负债	基础科目及衍生类因子
    "goods_sale_and_service_render_cash_ttm",		# 销售商品提供劳务收到的现金	基础科目及衍生类因子
    "gross_profit_ttm",		                        # 毛利TTM	基础科目及衍生类因子
    "interest_carry_current_liability",		        # 带息流动负债	基础科目及衍生类因子
    "interest_free_current_liability",		        # 无息流动负债	基础科目及衍生类因子
    "market_cap",		                # 市值	基础科目及衍生类因子
    "net_debt",		                    # 净债务	基础科目及衍生类因子
    "net_finance_cash_flow_ttm",		# 筹资活动现金流量净额TTM	基础科目及衍生类因子
    "net_interest_expense",		        # 净利息费用	基础科目及衍生类因子
    "net_invest_cash_flow_ttm",		    # 投资活动现金流量净额TTM	基础科目及衍生类因子
    "net_operate_cash_flow_ttm",		# 经营活动现金流量净额TTM	基础科目及衍生类因子
    "net_profit_ttm",		            # 净利润TTM	基础科目及衍生类因子
    "net_working_capital",		        # 净运营资本	基础科目及衍生类因子
    "non_operating_net_profit_ttm",		# 营业外收支净额TTM	基础科目及衍生类因子
    "non_recurring_gain_loss",		    # 非经常性损益	基础科目及衍生类因子
    "np_parent_company_owners_ttm",		# 归属于母公司股东的净利润TTM	基础科目及衍生类因子
    "OperateNetIncome",		            # 经营活动净收益	基础科目及衍生类因子
    "operating_assets",		            # 经营性资产	基础科目及衍生类因子
    "operating_cost_ttm",		        # 营业成本TTM	基础科目及衍生类因子
    "operating_liability",		        # 经营性负债	基础科目及衍生类因子
    "operating_profit_ttm",		        # 营业利润TTM	基础科目及衍生类因子
    "operating_revenue_ttm",		    # 营业收入TTM	基础科目及衍生类因子
    "retained_earnings",		        # 留存收益	基础科目及衍生类因子
    "sales_to_price_ratio",		        # 营收市值比	基础科目及衍生类因子
    "sale_expense_ttm",		            # 销售费用TTM	基础科目及衍生类因子
    "total_operating_cost_ttm",		    # 营业总成本TTM	基础科目及衍生类因子
    "total_operating_revenue_ttm",		# 营业总收入TTM	基础科目及衍生类因子
    "total_profit_ttm",		            # 利润总额TTM	基础科目及衍生类因子
    "value_change_profit_ttm",		    # 价值变动净收益TTM	基础科目及衍生类因子
    "AR",		                        # 人气指标	情绪类因子
    "ARBR",		                        # ARBR	情绪类因子
    "ATR14",		                    # 14日均幅指标	情绪类因子
    "ATR6",		                        # 6日均幅指标	情绪类因子
    "BR",		                        # 意愿指标	情绪类因子
    "DAVOL10",		                    # 10日平均换手率与120日平均换手率之比	情绪类因子
    "DAVOL20",		                    # 20日平均换手率与120日平均换手率之比	情绪类因子
    "DAVOL5",		                    # 5日平均换手率与120日平均换手率	情绪类因子
    "MAWVAD",		                    # 因子WVAD的6日均值	情绪类因子
    "money_flow_20",		            # 20日资金流量	情绪类因子
    "PSY",		                        # 心理线指标	情绪类因子
    "turnover_volatility",		        # 换手率相对波动率	情绪类因子
    "TVMA20",		                    # 20日成交金额的移动平均值	情绪类因子
    "TVMA6",		                    # 6日成交金额的移动平均值	情绪类因子
    "TVSTD20",		                    # 20日成交金额的标准差	情绪类因子
    "TVSTD6",		                    # 6日成交金额的标准差	情绪类因子
    "VDEA",		                        # 计算VMACD因子的中间变量	情绪类因子
    "VDIFF",		                    # 计算VMACD因子的中间变量	情绪类因子
    "VEMA10",		                    # 成交量的10日指数移动平均	情绪类因子
    "VEMA12",		                    # 12日成交量的移动平均值	情绪类因子
    "VEMA26",		                    # 成交量的26日指数移动平均	情绪类因子
    "VEMA5",		                    # 成交量的5日指数移动平均	情绪类因子
    "VMACD",		                    # 成交量指数平滑异同移动平均线	情绪类因子
    "VOL10",		                    # 10日平均换手率	情绪类因子
    "VOL120",		                    # 120日平均换手率	情绪类因子
    "VOL20",		                    # 20日平均换手率	情绪类因子
    "VOL240",		                    # 120日平均换手率	情绪类因子
    "VOL5",		                        # 5日平均换手率	情绪类因子
    "VOL60",		                    # 60日平均换手率	情绪类因子
    "VOSC",		                        # 成交量震荡	情绪类因子
    "VR",		                        # 成交量比率（Volume Ratio）	情绪类因子
    "VROC12",		                    # 12日量变动速率指标	情绪类因子
    "VROC6",		                    # 6日量变动速率指标	情绪类因子
    "VSTD10",		                    # 10日成交量标准差	情绪类因子
    "VSTD20",		                    # 20日成交量标准差	情绪类因子
    "WVAD",		                        # 威廉变异离散量	情绪类因子
    "financing_cash_growth_rate",		# 筹资活动产生的现金流量净额增长率	成长类因子
    "net_asset_growth_rate",		    # 净资产增长率	成长类因子
    "net_operate_cashflow_growth_rate",		    # 经营活动产生的现金流量净额增长率	成长类因子
    "net_profit_growth_rate",		            # 净利润增长率	成长类因子
    "np_parent_company_owners_growth_rate",		# 归属母公司股东的净利润增长率	成长类因子
    "operating_revenue_growth_rate",		    # 营业收入增长率	成长类因子
    "PEG",		                        # PEG	成长类因子
    "total_asset_growth_rate",		    # 总资产增长率	成长类因子
    "total_profit_growth_rate",		    # 利润总额增长率	成长类因子
    "arron_down_25",		            # Aroon指标下轨	动量类因子
    "arron_up_25",		                # Aroon指标上轨	动量类因子
    "BBIC",		                        # BBI 动量	动量类因子
    "bear_power",		                # 空头力道	动量类因子
    "BIAS10",		                    # 10日乖离率	动量类因子
    "BIAS20",		                    # 20日乖离率	动量类因子
    "BIAS5",		                    # 5日乖离率	动量类因子
    "BIAS60",		                    # 60日乖离率	动量类因子
    "bull_power",		                # 多头力道	动量类因子
    "CCI10",		                    # 10日顺势指标	动量类因子
    "CCI15",		                    # 15日顺势指标	动量类因子
    "CCI20",		                    # 20日顺势指标	动量类因子
    "CCI88",		                    # 88日顺势指标	动量类因子
    "CR20",		                        # CR指标	动量类因子
    "fifty_two_week_close_rank",		# 当前价格处于过去1年股价的位置	动量类因子
    "MASS",		                        # 梅斯线	动量类因子
    "PLRC12",		                    # 12日收盘价格与日期线性回归系数	动量类因子
    "PLRC24",		                    # 24日收盘价格与日期线性回归系数	动量类因子
    "PLRC6",		                    # 6日收盘价格与日期线性回归系数	动量类因子
    "Price1M",		                    # 当前股价除以过去一个月股价均值再减1	动量类因子
    "Price1Y",		                    # 当前股价除以过去一年股价均值再减1	动量类因子
    "Price3M",		                    # 当前股价除以过去三个月股价均值再减1	动量类因子
    "Rank1M",		                    # 1减去 过去一个月收益率排名与股票总数的比值	动量类因子
    "ROC12",		                    # 12日变动速率（Price Rate of Change）	动量类因子
    "ROC120",		                    # 120日变动速率（Price Rate of Change）	动量类因子
    "ROC20",		                    # 20日变动速率（Price Rate of Change）	动量类因子
    "ROC6",		                        # 6日变动速率（Price Rate of Change）	动量类因子
    "ROC60",		                    # 60日变动速率（Price Rate of Change）	动量类因子
    "single_day_VPT",		            # 单日价量趋势	动量类因子
    "single_day_VPT_12",		        # 单日价量趋势12均值	动量类因子
    "single_day_VPT_6",		            # 单日价量趋势6日均值	动量类因子
    "TRIX10",		                    # 10日终极指标TRIX	动量类因子
    "TRIX5",		                    # 5日终极指标TRIX	动量类因子
    "Volume1M",		                    # 当前交易量相比过去1个月日均交易量 与过去过去20日日均收益率乘积	动量类因子
    "capital_reserve_fund_per_share",		# 每股资本公积金	每股指标因子
    "cashflow_per_share_ttm",		        # 每股现金流量净额，根据当时日期来获取最近变更日的总股本	每股指标因子
    "cash_and_equivalents_per_share",		# 每股现金及现金等价物余额	每股指标因子
    "eps_ttm",		                        # 每股收益TTM	每股指标因子
    "net_asset_per_share",		            # 每股净资产	每股指标因子
    "net_operate_cash_flow_per_share",		# 每股经营活动产生的现金流量净额	每股指标因子
    "operating_profit_per_share",		    # 每股营业利润	每股指标因子
    "operating_profit_per_share_ttm",		# 每股营业利润TTM	每股指标因子
    "operating_revenue_per_share",		    # 每股营业收入	每股指标因子
    "operating_revenue_per_share_ttm",		# 每股营业收入TTM	每股指标因子
    "retained_earnings_per_share",		    # 每股留存收益	每股指标因子
    "retained_profit_per_share",		    # 每股未分配利润	每股指标因子
    "surplus_reserve_fund_per_share",		# 每股盈余公积金	每股指标因子
    "total_operating_revenue_per_share",		    # 每股营业总收入	每股指标因子
    "total_operating_revenue_per_share_ttm",		# 每股营业总收入TTM	每股指标因子
    "ACCA",		                                    # 现金流资产比和资产回报率之差	质量类因子
    "accounts_payable_turnover_days",		# 应付账款周转天数	质量类因子
    "accounts_payable_turnover_rate",		# 应付账款周转率	质量类因子
    "account_receivable_turnover_days",		# 应收账款周转天数	质量类因子
    "account_receivable_turnover_rate",		# 应收账款周转率	质量类因子
    "adjusted_profit_to_total_profit",		# 扣除非经常损益后的净利润/净利润	质量类因子
    "admin_expense_rate",		            # 管理费用与营业总收入之比	质量类因子
    "asset_turnover_ttm",		            # 经营资产周转率TTM	质量类因子
    "cash_rate_of_sales",		            # 经营活动产生的现金流量净额与营业收入之比	质量类因子
    "cash_to_current_liability",		    # 现金比率	质量类因子
    "cfo_to_ev",		                    # 经营活动产生的现金流量净额与企业价值之比TTM	质量类因子
    "current_asset_turnover_rate",		    # 流动资产周转率TTM	质量类因子
    "current_ratio",		                # 流动比率(单季度)	质量类因子
    "debt_to_asset_ratio",		            # 债务总资产比	质量类因子
    "debt_to_equity_ratio",		            # 产权比率	质量类因子
    "debt_to_tangible_equity_ratio",		# 有形净值债务率	质量类因子
    "DEGM",		                            # 毛利率增长	质量类因子
    "DEGM_8y",		                        # 长期毛利率增长	质量类因子
    "DSRI",		                            # 应收账款指数	质量类因子
    "equity_to_asset_ratio",		        # 股东权益比率	质量类因子
    "equity_to_fixed_asset_ratio",		    # 股东权益与固定资产比率	质量类因子
    "equity_turnover_rate",		            # 股东权益周转率	质量类因子
    "financial_expense_rate",		        # 财务费用与营业总收入之比	质量类因子
    "fixed_assets_turnover_rate",		    # 固定资产周转率	质量类因子
    "fixed_asset_ratio",		            # 固定资产比率	质量类因子
    "GMI",		                            # 毛利率指数	质量类因子
    "goods_service_cash_to_operating_revenue_ttm",		# 销售商品提供劳务收到的现金与营业收入之比	质量类因子
    "gross_income_ratio",		                        # 销售毛利率	质量类因子
    "intangible_asset_ratio",		                    # 无形资产比率	质量类因子
    "inventory_turnover_days",		                    # 存货周转天数	质量类因子
    "inventory_turnover_rate",		                    # 存货周转率	质量类因子
    "invest_income_associates_to_total_profit",		    # 对联营和合营公司投资收益/利润总额	质量类因子
    "long_debt_to_asset_ratio",		                    # 长期借款与资产总计之比	质量类因子
    "long_debt_to_working_capital_ratio",		        # 长期负债与营运资金比率	质量类因子
    "long_term_debt_to_asset_ratio",		            # 长期负债与资产总计之比	质量类因子
    "LVGI",		                                        # 财务杠杆指数	质量类因子
    "margin_stability",		                            # 盈利能力稳定性	质量类因子
    "maximum_margin",		                            # 最大盈利水平	质量类因子
    "MLEV",		                                        # 市场杠杆	质量类因子
    "net_non_operating_income_to_total_profit",		    # 营业外收支利润净额/利润总额	质量类因子
    "net_operate_cash_flow_to_asset",		            # 总资产现金回收率	质量类因子
    "net_operate_cash_flow_to_net_debt",		        # 经营活动产生现金流量净额/净债务	质量类因子
    "net_operate_cash_flow_to_operate_income",		        # 经营活动产生的现金流量净额与经营活动净收益之比	质量类因子
    "net_operate_cash_flow_to_total_current_liability",		# 现金流动负债比	质量类因子
    "net_operate_cash_flow_to_total_liability",		        # 经营活动产生的现金流量净额/负债合计	质量类因子
    "net_operating_cash_flow_coverage",		                # 净利润现金含量	质量类因子
    "net_profit_ratio",		                                # 销售净利率	质量类因子
    "net_profit_to_total_operate_revenue_ttm",		        # 净利润与营业总收入之比	质量类因子
    "non_current_asset_ratio",		                        # 非流动资产比率	质量类因子
    "OperatingCycle",		                                # 营业周期	质量类因子
    "operating_cost_to_operating_revenue_ratio",		    # 销售成本率	质量类因子
    "operating_profit_growth_rate",		                    # 营业利润增长率	质量类因子
    "operating_profit_ratio",		                        # 营业利润率	质量类因子
    "operating_profit_to_operating_revenue",		        # 营业利润与营业总收入之比	质量类因子
    "operating_profit_to_total_profit",		                # 经营活动净收益/利润总额	质量类因子
    "operating_tax_to_operating_revenue_ratio_ttm",		    # 销售税金率	质量类因子
    "profit_margin_ttm",		                            # 销售利润率TTM	质量类因子
    "quick_ratio",		                                    # 速动比率	质量类因子
    "rnoa_ttm",		                                        # 经营资产回报率TTM	质量类因子
    "ROAEBITTTM",		                                    # 总资产报酬率	质量类因子
    "roa_ttm",		                            # 资产回报率TTM	质量类因子
    "roa_ttm_8y",		                        # 长期资产回报率TTM	质量类因子
    "roe_ttm",		                            # 权益回报率TTM	质量类因子
    "roe_ttm_8y",		                        # 长期权益回报率TTM	质量类因子
    "roic_ttm",		                            # 投资资本回报率TTM	质量类因子
    "sale_expense_to_operating_revenue",		# 营业费用与营业总收入之比	质量类因子
    "SGAI",		                                # 销售管理费用指数	质量类因子
    "SGI",		                                # 营业收入指数	质量类因子
    "super_quick_ratio",		                # 超速动比率	质量类因子
    "total_asset_turnover_rate",		        # 总资产周转率	质量类因子
    "total_profit_to_cost_ratio",		        # 成本费用利润率	质量类因子
    "Kurtosis120",		                        # 个股收益的120日峰度	风险类因子
    "Kurtosis20",		                        # 个股收益的20日峰度	风险类因子
    "Kurtosis60",		                        # 个股收益的60日峰度	风险类因子
    "sharpe_ratio_120",		                    # 120日夏普比率	风险类因子
    "sharpe_ratio_20",		                    # 20日夏普比率	风险类因子
    "sharpe_ratio_60",		                    # 60日夏普比率	风险类因子
    "Skewness120",		                        # 个股收益的120日偏度	风险类因子
    "Skewness20",		                        # 个股收益的20日偏度	风险类因子
    "Skewness60",		                        # 个股收益的60日偏度	风险类因子
    "Variance120",		                        # 120日收益方差	风险类因子
    "Variance20",		                        # 20日收益方差	风险类因子
    "Variance60",		                        # 60日收益方差	风险类因子
    "average_share_turnover_annual",		    # 年度平均月换手率	风险因子 - 风格因子
    "average_share_turnover_quarterly",		    # 季度平均平均月换手率	风险因子 - 风格因子
    "beta",		                                # BETA	风险因子 - 风格因子
    "book_leverage",		                    # 账面杠杆	风险因子 - 风格因子
    "book_to_price_ratio",		                # 市净率因子	风险因子 - 风格因子
    "cash_earnings_to_price_ratio",		        # 现金流量市值比	风险因子 - 风格因子
    "cube_of_size",		                        # 市值立方因子	风险因子 - 风格因子
    "cumulative_range",		                    # 收益离差	风险因子 - 风格因子
    "daily_standard_deviation",		            # 日收益率标准差	风险因子 - 风格因子
    "debt_to_assets",		                    # 资产负债率	风险因子 - 风格因子
    "earnings_growth",		                    # 5年盈利增长率	风险因子 - 风格因子
    "earnings_to_price_ratio",		            # 利润市值比	风险因子 - 风格因子
    "earnings_yield",		                    # 盈利预期因子	风险因子 - 风格因子
    "growth",		                            # 成长因子	风险因子 - 风格因子
    "historical_sigma",		                    # 残差历史波动率	风险因子 - 风格因子
    "leverage",		                            # 杠杆因子	风险因子 - 风格因子
    "liquidity",		                        # 流动性因子	风险因子 - 风格因子
    "long_term_predicted_earnings_growth",		# 预期长期盈利增长率	风险因子 - 风格因子
    "market_leverage",		                    # 市场杠杆	风险因子 - 风格因子
    "momentum",		                            # 动量因子	风险因子 - 风格因子
    "natural_log_of_market_cap",		        # 对数总市值	风险因子 - 风格因子
    "non_linear_size",		                    # 非线性市值因子	风险因子 - 风格因子
    "predicted_earnings_to_price_ratio",		# 预期市盈率	风险因子 - 风格因子
    "raw_beta",		                            # RAW BETA	风险因子 - 风格因子
    "relative_strength",		                # 相对强弱	风险因子 - 风格因子
    "residual_volatility",		                # 残差波动因子	风险因子 - 风格因子
    "sales_growth",		                        # 5年营业收入增长率	风险因子 - 风格因子
    "share_turnover_monthly",		            # 月换手率	风险因子 - 风格因子
    "short_term_predicted_earnings_growth",		# 预期短期盈利增长率	风险因子 - 风格因子
    "size",		                                # 市值因子	风险因子 - 风格因子
    "boll_down",		                        # 下轨线（布林线）指标	技术指标因子
    "boll_up",		                            # 上轨线（布林线）指标	技术指标因子
    "EMA5",		                                # 5日指数移动均线	技术指标因子
    "EMAC10",		                            # 10日指数移动均线	技术指标因子
    "EMAC12",		                            # 12日指数移动均线	技术指标因子
    "EMAC120",		                            # 120日指数移动均线	技术指标因子
    "EMAC20",		                            # 20日指数移动均线	技术指标因子
    "EMAC26",		                            # 26日指数移动均线	技术指标因子
    "MAC10",		                            # 10日移动均线	技术指标因子
    "MAC120",		                            # 120日移动均线	技术指标因子
    "MAC20",		                            # 20日移动均线	技术指标因子
    "MAC5",		                                # 5日移动均线	技术指标因子
    "MAC60",		                            # 60日移动均线	技术指标因子
    "MACDC",		                            # 平滑异同移动平均线	技术指标因子
    "MFI14",		                            # 资金流量指标	技术指标因子
]
