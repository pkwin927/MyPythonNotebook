# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 22:52:07 2019

@author: genio
"""


import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from statsmodels.distributions.empirical_distribution import ECDF
from sklearn.model_selection import train_test_split
import math
#pd.options.mode.chained_assignment = None





def FindNull(data):
    
    NullCounts = data.isnull().sum().values
    
    Total = data.shape[0]
    
    Ratio = NullCounts / Total
    
    return(pd.DataFrame({"NullCounts":NullCounts,
                         "TotalData":Total,
                         "NullRatio":Ratio},
                        index = data.columns.values))

def FindZero(DF):
    
    ZeroCounts = (DF == 0).sum().values
    
    Total = DF.shape[0]
    
    Ratio = ZeroCounts / Total
    
    return(pd.DataFrame({"ZeroCounts":ZeroCounts,
                         "TotalData":Total,
                         "ZeroRatio":Ratio},
                        index = DF.columns.values))

def IQRScore(data,targetcols = None,classification = None,label = None,plot = False,figsize = (10,5),xlim = None):
    
    if classification == None and label != None:
        
        classification = label
        
        label = None
    
    if targetcols == None:
        
        SelectColumns = data.columns.values
        
    else:
                
        SelectColumns = targetcols
    
    if classification == None and label == None:
        
        DF = data[SelectColumns]
        
        Minimum = DF.min()
        
        Maximum = DF.max()
        
        Q1 = DF.quantile(0.25)
        
        Q3 = DF.quantile(0.75)
        
        IQR = Q3 - Q1
        
        Outliers = DF[(DF < (Q1 -1.5*IQR)) | (DF > (Q3 + 1.5*IQR))].count()
                
        Result = pd.DataFrame({"Min":Minimum,
                               "Q1":Q1,
                               "Q3":Q3,
                               "Max":Maximum,
                               "Q1 - 1.5IQR":Q1 - 1.5 * IQR,
                               "Q3 + 1.5IQR":Q3 + 1.5 * IQR,
                               "IQR_Score":IQR,
                               "Outliers_Counts":Outliers},index = SelectColumns)
    elif classification != None:
            
        Result = pd.DataFrame(columns = {"Q1","Q3","Q1 - 1.5IQR", "Q3 + 1.5IQR", "IQR_Score","Outliers_Counts",classification}) 

        if label == None:
        
            for Class in data[classification].unique():
                
                DF = data.loc[data[classification] == Class]
                
                DF = DF[SelectColumns]
                
                Q1 = DF.quantile(0.25)
                
                Q3 = DF.quantile(0.75)
                
                IQR = Q3 - Q1
                
                Outliers = DF[(DF < (Q1 -1.5*IQR)) | (DF > (Q3 + 1.5*IQR))].count()
                    
                ResultTemp = pd.DataFrame({classification:Class,
                                       "Q1":Q1,
                                       "Q3":Q3,
                                       "Q1 - 1.5IQR":Q1 - 1.5 * IQR,
                                       "Q3 + 1.5IQR":Q3 + 1.5 * IQR,
                                       "IQR_Score":IQR,
                                       "Outliers_Counts":Outliers},index = SelectColumns)
        
                Result = pd.concat([Result,ResultTemp], axis = 0,sort = True)
        
        else:
            
                for LabelValue in data[label].unique():
                    
                    for Class in data[classification].unique():
                        
                        DF = data.loc[(data[label] == LabelValue) & (data[classification] == Class)]
                        
                        DF = DF[SelectColumns]
                        
                        Q1 = DF.quantile(0.25)
                        
                        Q3 = DF.quantile(0.75)
                        
                        IQR = Q3 - Q1
                        
                        Outliers = DF[(DF < (Q1 -1.5*IQR)) | (DF > (Q3 + 1.5*IQR))].count()
                            
                        ResultTemp = pd.DataFrame({classification:Class,
                                                   label:LabelValue,
                                                   "Q1":Q1,
                                                   "Q3":Q3,
                                                   "Q1 - 1.5IQR":Q1 - 1.5 * IQR,
                                                   "Q3 + 1.5IQR":Q3 + 1.5 * IQR,
                                                   "IQR_Score":IQR,
                                                   "Outliers_Counts":Outliers},index = SelectColumns)
                
                        Result = pd.concat([Result,ResultTemp], axis = 0,sort = True)
    
    else:
        
        pass
    
    if plot == True:
        
        for col in SelectColumns:
            plt.figure(figsize = figsize)
            
            sns.boxplot(x = classification,y = col,hue = label,data = data, orient = 'v')
       
            if xlim == None:
                pass
            else:
                plt.xlim(xlim)
            
    else:
        
        pass
    
    return(Result)

def PlotNumerical(DF, targetcols = None, figsize = (10,5), ticksfontsize = 12, titlefontsize = 20, kde = True):
    
    if targetcols == None:
        
        SelectColumns = DF.columns.values
        
    else:
                
        SelectColumns = targetcols
    
    for col in SelectColumns:
        
#        col = "OWN_CAR_AGE"
        
        plt.figure(figsize = figsize)
        
        plt.suptitle(col,fontsize = titlefontsize,y = 0.91)
        
        plt.subplot(221)
        plt.grid()
        plt.xticks(fontsize = ticksfontsize)
        plt.yticks(fontsize = ticksfontsize)
        try:
            sns.distplot(DF[col], kde = kde)
        except:
            sns.distplot(DF[col], kde = False)
        
        plt.subplot(222)
        plt.grid()
        plt.xticks(fontsize = ticksfontsize)
        plt.yticks(fontsize = ticksfontsize)
        sns.boxplot(x = col, data = DF)

        plt.subplot(212)
        plt.grid()
        plt.xticks(fontsize = ticksfontsize)
        plt.yticks(fontsize = ticksfontsize)
#        sns.distplot(DF[col],rug_kws = {"cumulative" : True}, kde_kws = {"cumulative":True})
        ecdf = ECDF(DF[col])
        plt.plot(ecdf.x, ecdf.y / ecdf.y.max())
#        sns.distplot(DF[col], rug_kws = {"cumulative":True})
        plt.xlabel('Value')
        plt.ylabel('ECDF')

        plt.show()
        
def corr(data, targetcols = None, classcol = None,method = 'spearman', min_periods = 1, plot = False, plot_gride = False, stratify = False, random_seed = None, sample_size = 0.2,figsize = (10,5)):
    
    method = method.lower()
    
    if targetcols == None:
        
        SelectColumns = data.columns.values
    
    else:
        
        SelectColumns = targetcols.copy()
        
    if classcol == None:
        
        pass
    
    else:
        
        SelectColumns.append(classcol)
        
    CorrResult = data[SelectColumns].corr(method = method, min_periods = min_periods)
    
    if plot == True:
        
        plt.figure(figsize = figsize)
        
        plt.subplot(221)
        sns.heatmap(CorrResult, cmap = plt.cm.RdYlBu_r, vmin = -0.25, annot = True, vmax = 0.6)
        
        if plot_gride == True:
            
            data = data.dropna()
            
            if stratify == True:
                
                PlotData = train_test_split(data[SelectColumns], test_size= sample_size, random_state=random_seed, stratify=data[[classcol]])[1]
            
            else:
                
                PlotData = data[SelectColumns]
                
            plt.subplot(222)
    #        sns.pairplot(data = data[SelectColumns], hue = classification, diag_kind = 'kde', kind = 'reg')
    
            grid = sns.PairGrid(data = PlotData, size = 3, diag_sharey=False,
                                hue = classcol, vars = targetcols)
            
            grid.map_upper(plt.scatter, alpha = 0.2)
            
            grid.map_diag(sns.kdeplot)
            
            grid.map_lower(sns.kdeplot, cmap = plt.cm.OrRd_r)
            
        plt.show()
    else:
        
        pass
    
    return(CorrResult)
    
def PlotCategorical(data, labelcols = None ,targetcols = None, classcol = None, col = None,figsize = (10,5)):
    
    
    if targetcols == None:
        
        if labelcols == None:
            SelectColumns = data.columns
        else:
            SelectColumns = np.delete(data.columns.values,np.where(data.columns.values == labelcols))
        
    else:
                
        SelectColumns = targetcols
    
    if labelcols == None:
        
        for SelectCol in SelectColumns:

            plt.figure(figsize = figsize)
            sns.catplot(x= SelectCol, 
                         hue=classcol, col=col,
                         data=data, kind='count',
                         aspect=2,height = 5)      
            plt.show()
        
    else:
        
        for SelectCol in SelectColumns:
            
            for plotlabel in labelcols:
            
                plt.figure(figsize = figsize)

                sns.catplot(x= SelectCol, y= plotlabel,
                             hue=classcol, col=col,
                             data=data, kind='bar',
                             aspect=2,height = 5)
                
                plt.show()

def Plotkde(data,targetcols = None, classcols = None, title = None, figsize = (10,5), fontsize = 12):
    
    if targetcols == None:
        
        SelectColumns = np.delete(data.columns.values,np.where(data.columns.values == classcols))

    else:
        
        SelectColumns = targetcols
        
        
    for col in SelectColumns:
        

        if classcols == None:
            
            plt.figure(figsize = figsize)
            
            plt.title(title, fontsize = fontsize)

            sns.kdeplot(data[col])
            
            plt.show()
            
        else:
            
            for classes in  classcols:
                
                plt.figure(figsize = figsize)
                
                plt.title(col + '_' + classes, fontsize = fontsize)
                
                classname = data[classes].unique()
        
                for name in classname:
                    
                    sns.kdeplot(data.loc[data[classes] == name,col], label = name)
                    
                plt.show()
        
                    
   
def PlotScatter(data, labelcols,targetcols = None, classcol = None, col = None,figsize = (10,5),logistic = False):
    
    if targetcols == None:
    
        SelectColumns = np.delete(data.columns.values,np.where(data.columns.values == labelcols))
        
    else:
                
        SelectColumns = targetcols
        
    for SelectCol in SelectColumns:
        
        for plotlabel in labelcols:
            
            plt.figure(figsize = figsize)
    
            sns.lmplot(x= SelectCol, y= plotlabel,
                         hue=classcol, col=col,
                         data=data,logistic = logistic)
            
            plt.show()
            
            
def PlotProportion(data, labelcol,targetcols = None , figsize = (8,8) ):
    
    if targetcols == None:
        
        SelectColumns = np.delete(data.columns.values,np.where(data.columns.values == labelcol))

    else:
        
        SelectColumns = targetcols
        
    for col in SelectColumns:
    
        fig, axs = plt.subplots(math.ceil(len(data[col].unique()) / 2), 2, figsize=figsize)
        
        axs = axs.flatten()
        
        for i, ax in enumerate(axs, start=0):
            
            ColorList = [ "red", "blue", "green", "yellow", "purple", "orange", "black",'aqua','hotpink','violet','chartreuse' ]
    
            data.loc[data[col] == data[col].unique()[i], labelcol].value_counts().sort_index().plot("bar", ax=ax,color = ColorList)
            
            ax.set_title(data[col].unique()[i])
            
        plt.show()
            
            
##################################################################################
## Featexp
            
            
def get_grouped_data(input_data, feature, target_col, bins, cuts=0):
    """
    Bins continuous features into equal sample size buckets and 
    returns the target mean in each bucket. Separates out nulls into 
    another bucket.
    
    :param input_data: dataframe containg features and target column.
    :param feature: feature column name.
    :param target_col: target column.
    :param bins: Number bins required.
    :param cuts: if buckets of certain specific cuts are required. Used 
    on test data to use cuts from train.
    :return: If cuts are passed only grouped data is returned, else cuts 
    and grouped data is returned.
    """
    
    has_null = pd.isnull(input_data[feature]).sum() > 0
    if has_null == 1:
        data_null = input_data[pd.isnull(input_data[feature])]
        input_data = input_data[~pd.isnull(input_data[feature])]
        input_data.reset_index(inplace=True, drop=True)

    is_train = 0
    if cuts == 0:
        is_train = 1
        prev_cut = min(input_data[feature]) - 1
        cuts = [prev_cut]
        reduced_cuts = 0
        for i in range(1, bins + 1):
            next_cut = np.percentile(input_data[feature], i * 100 / bins)
            if (
                next_cut > prev_cut + 0.000001
            ):  # float numbers shold be compared with some threshold!
                cuts.append(next_cut)
            else:
                reduced_cuts = reduced_cuts + 1
            prev_cut = next_cut

        # if reduced_cuts>0:
        #     print('Reduced the number of bins due to less variation in feature')
        cut_series = pd.cut(input_data[feature], cuts)
    else:
        cut_series = pd.cut(input_data[feature], cuts)

    grouped = input_data.groupby([cut_series], as_index=True).agg(
        {target_col: [np.size, np.mean], feature: [np.mean]}
    )
    grouped.columns = [
        "_".join(cols).strip() for cols in grouped.columns.values
    ]
    grouped[grouped.index.name] = grouped.index
    grouped.reset_index(inplace=True, drop=True)
    grouped = grouped[[feature] + list(grouped.columns[0:3])]
    grouped = grouped.rename(
        index=str, columns={target_col + "_size": "Samples_in_bin"}
    )
    grouped = grouped.reset_index(drop=True)
    corrected_bin_name = (
        "["
        + str(min(input_data[feature]))
        + ", "
        + str(grouped.loc[0, feature]).split(",")[1]
    )
    grouped[feature] = grouped[feature].astype("category")
    grouped[feature] = grouped[feature].cat.add_categories(corrected_bin_name)
    grouped.loc[0, feature] = corrected_bin_name

    if has_null == 1:
        grouped_null = grouped.loc[0:0, :].copy()
        grouped_null[feature] = grouped_null[feature].astype("category")
        grouped_null[feature] = grouped_null[feature].cat.add_categories(
            "Nulls"
        )
        grouped_null.loc[0, feature] = "Nulls"
        grouped_null.loc[0, "Samples_in_bin"] = len(data_null)
        grouped_null.loc[0, target_col + "_mean"] = data_null[
            target_col
        ].mean()
        grouped_null.loc[0, feature + "_mean"] = np.nan
        grouped[feature] = grouped[feature].astype("str")
        grouped = pd.concat([grouped_null, grouped], axis=0)
        grouped.reset_index(inplace=True, drop=True)

    grouped[feature] = grouped[feature].astype("str").astype("category")
    if is_train == 1:
        return (cuts, grouped)
    else:
        return grouped


def draw_plots(input_data, feature, target_col, trend_correlation=None):
    """
    Draws univariate dependence plots for a feature.
    
    :param input_data: grouped data contained bins of feature and 
    target mean.
    :param feature: feature column name.
    :param target_col: target column.
    :param trend_correlation: correlation between train and test trends 
    of feature wrt target.
    :return: Draws trend plots for feature.
    """
    
    trend_changes = get_trend_changes(
        grouped_data=input_data, feature=feature, target_col=target_col
    )
    plt.figure(figsize=(12, 5))
    ax1 = plt.subplot(1, 2, 1)
    ax1.plot(input_data[target_col + "_mean"], marker="o")
    ax1.set_xticks(np.arange(len(input_data)))
    ax1.set_xticklabels((input_data[feature]).astype("str"))
    plt.xticks(rotation=45)
    ax1.set_xlabel("Bins of " + feature)
    ax1.set_ylabel("Average of " + target_col)
    comment = "Trend changed " + str(trend_changes) + " times"
    if trend_correlation == 0:
        comment = comment + "\n" + "Correlation with train trend: NA"
    elif trend_correlation != None:
        comment = (
            comment
            + "\n"
            + "Correlation with train trend: "
            + str(int(trend_correlation * 100))
            + "%"
        )

    props = dict(boxstyle="round", facecolor="wheat", alpha=0.3)
    ax1.text(
        0.05,
        0.95,
        comment,
        fontsize=12,
        verticalalignment="top",
        bbox=props,
        transform=ax1.transAxes,
    )
    plt.title("Average of " + target_col + " wrt " + feature)

    ax2 = plt.subplot(1, 2, 2)
    ax2.bar(
        np.arange(len(input_data)), input_data["Samples_in_bin"], alpha=0.5
    )
    ax2.set_xticks(np.arange(len(input_data)))
    ax2.set_xticklabels((input_data[feature]).astype("str"))
    plt.xticks(rotation=45)
    ax2.set_xlabel("Bins of " + feature)
    ax2.set_ylabel("Bin-wise sample size")
    plt.title("Samples in bins of " + feature)
    plt.tight_layout()
    plt.show()


def get_trend_changes(grouped_data, feature, target_col, threshold=0.03):
    """
    Calculates number of times the trend of feature wrt target changed
    direction.
    
    :param grouped_data: grouped dataset.
    :param feature: feature column name.
    :param target_col: target column.
    :param threshold: minimum % difference required to count as trend 
    change.
    :return: number of trend chagnes for the feature.
    """
    
    grouped_data = grouped_data.loc[
        grouped_data[feature] != "Nulls", :
    ].reset_index(drop=True)
    target_diffs = grouped_data[target_col + "_mean"].diff()
    target_diffs = target_diffs[~np.isnan(target_diffs)].reset_index(drop=True)
    max_diff = (
        grouped_data[target_col + "_mean"].max()
        - grouped_data[target_col + "_mean"].min()
    )
    target_diffs_mod = target_diffs.fillna(0).abs()
    low_change = target_diffs_mod < threshold * max_diff
    target_diffs_norm = target_diffs.divide(target_diffs_mod)
    target_diffs_norm[low_change] = 0
    target_diffs_norm = target_diffs_norm[target_diffs_norm != 0]
    target_diffs_lvl2 = target_diffs_norm.diff()
    changes = target_diffs_lvl2.fillna(0).abs() / 2
    tot_trend_changes = int(changes.sum()) if ~np.isnan(changes.sum()) else 0
    return tot_trend_changes


def get_trend_correlation(grouped, grouped_test, feature, target_col):
    """
    Calculates correlation between train and test trend of feature 
    wrt target.
    
    :param grouped: train grouped data.
    :param grouped_test: test grouped data.
    :param feature: feature column name.
    :param target_col: target column name.
    :return: trend correlation between train and test.
    """
    
    grouped = grouped[grouped[feature] != "Nulls"].reset_index(drop=True)
    grouped_test = grouped_test[grouped_test[feature] != "Nulls"].reset_index(
        drop=True
    )

    if grouped_test.loc[0, feature] != grouped.loc[0, feature]:
        grouped_test[feature] = grouped_test[feature].cat.add_categories(
            grouped.loc[0, feature]
        )
        grouped_test.loc[0, feature] = grouped.loc[0, feature]
    grouped_test_train = grouped.merge(
        grouped_test[[feature, target_col + "_mean"]],
        on=feature,
        how="left",
        suffixes=("", "_test"),
    )
    nan_rows = pd.isnull(grouped_test_train[target_col + "_mean"]) | pd.isnull(
        grouped_test_train[target_col + "_mean_test"]
    )
    grouped_test_train = grouped_test_train.loc[~nan_rows, :]
    if len(grouped_test_train) > 1:
        trend_correlation = np.corrcoef(
            grouped_test_train[target_col + "_mean"],
            grouped_test_train[target_col + "_mean_test"],
        )[0, 1]
    else:
        trend_correlation = 0
        print(
            "Only one bin created for "
            + feature
            + ". Correlation can't be calculated"
        )

    return trend_correlation


def univariate_plotter(feature, data, target_col, bins=10, data_test=0):
    """
    Calls the draw plot function and editing around the plots.
    
    :param feature: feature column name.
    :param data: dataframe containing features and target columns.
    :param target_col: target column name.
    :param bins: number of bins to be created from continuous feature.
    :param data_test: test data which has to be compared with input data 
    for correlation.
    :return: grouped data if only train passed, else (grouped train 
    data, grouped test data).
    """
    
    print(" {:^100} ".format("Plots for " + feature))
    if data[feature].dtype == "O":
        print("Categorical feature not supported")
    else:
        cuts, grouped = get_grouped_data(
            input_data=data, feature=feature, target_col=target_col, bins=bins
        )
        has_test = type(data_test) == pd.core.frame.DataFrame
        if has_test:
            grouped_test = get_grouped_data(
                input_data=data_test.reset_index(drop=True),
                feature=feature,
                target_col=target_col,
                bins=bins,
                cuts=cuts,
            )
            trend_corr = get_trend_correlation(
                grouped, grouped_test, feature, target_col
            )
            print(" {:^100} ".format("Train data plots"))

            draw_plots(
                input_data=grouped, feature=feature, target_col=target_col
            )
            print(" {:^100} ".format("Test data plots"))

            draw_plots(
                input_data=grouped_test,
                feature=feature,
                target_col=target_col,
                trend_correlation=trend_corr,
            )
        else:
            draw_plots(
                input_data=grouped, feature=feature, target_col=target_col
            )
        print(
            "----------------------------------------------------------"
            "----------------------------------------------------"
        )
        print("\n")
        if has_test:
            return (grouped, grouped_test)
        else:
            return grouped


def get_univariate_plots(
    data, target_col, features_list=0, bins=10, data_test=0
):
    """
    Creates univariate dependence plots for features in the dataset
    :param data: dataframe containing features and target columns
    :param target_col: target column name
    :param features_list: by default creates plots for all features. If 
    list passed, creates plots of only those features
    :param bins: number of bins to be created from continuous feature
    :param data_test: test data which has to be compared with input 
    data for correlation
    :return: Draws univariate plots for all columns in data
    """
    if type(features_list) == int:
        features_list = list(data.columns)
        features_list.remove(target_col)

    for cols in features_list:
        if cols != target_col and data[cols].dtype == "O":
            print(
                cols
                + " is categorical. Categorical features not supported yet."
            )
        elif cols != target_col and data[cols].dtype != "O":
            univariate_plotter(
                feature=cols,
                data=data,
                target_col=target_col,
                bins=bins,
                data_test=data_test,
            )


def get_trend_stats(data, target_col, features_list=0, bins=10, data_test=0):
    """
    Calculates trend changes and correlation between train/test for 
    list of features.
    
    :param data: dataframe containing features and target columns.
    :param target_col: target column name.
    :param features_list: by default creates plots for all features. If 
    list passed, creates plots of only those features.
    :param bins: number of bins to be created from continuous feature.
    :param data_test: test data which has to be compared with input data 
    for correlation.
    :return: dataframe with trend changes and trend correlation 
    (if test data passed).
    """

    if type(features_list) == int:
        features_list = list(data.columns)
        features_list.remove(target_col)

    stats_all = []
    has_test = type(data_test) == pd.core.frame.DataFrame
    ignored = []
    for feature in features_list:
        if data[feature].dtype == "O" or feature == target_col:
            ignored.append(feature)
        else:
            cuts, grouped = get_grouped_data(
                input_data=data,
                feature=feature,
                target_col=target_col,
                bins=bins,
            )
            trend_changes = get_trend_changes(
                grouped_data=grouped, feature=feature, target_col=target_col
            )
            if has_test:
                grouped_test = get_grouped_data(
                    input_data=data_test.reset_index(drop=True),
                    feature=feature,
                    target_col=target_col,
                    bins=bins,
                    cuts=cuts,
                )
                trend_corr = get_trend_correlation(
                    grouped, grouped_test, feature, target_col
                )
                trend_changes_test = get_trend_changes(
                    grouped_data=grouped_test,
                    feature=feature,
                    target_col=target_col,
                )
                stats = [
                    feature,
                    trend_changes,
                    trend_changes_test,
                    trend_corr,
                ]
            else:
                stats = [feature, trend_changes]
            stats_all.append(stats)
    stats_all_df = pd.DataFrame(stats_all)
    stats_all_df.columns = (
        ["Feature", "Trend_changes"]
        if has_test == False
        else [
            "Feature",
            "Trend_changes",
            "Trend_changes_test",
            "Trend_correlation",
        ]
    )
    if len(ignored) > 0:
        print(
            "Categorical features "
            + str(ignored)
            + " ignored. Categorical features not supported yet."
        )

    print("Returning stats for all numeric features")
    return stats_all_df


def DataSetInfo(DF):
    
    TotalMemory = sum(DF.memory_usage(deep = True))
    
    MemoryType = 0
    
    MemoryList = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        
    while TotalMemory >1024 and MemoryType < 6:
        
        TotalMemory = TotalMemory / 1024
        
        MemoryType += 1
        
    MemoryStr = str(round(TotalMemory,2)) +' '+ MemoryList[MemoryType]
    
    NullDF = FindNull(DF)
    
    ZeroDF = FindZero(DF)
    
    print('##################################################')
    
    print('Number of columns:',DF.shape[1])
    
    print('Number of rows:', DF.shape[0])
    
    print('Missing rows:',DF[DF.isnull().values == True].shape[0])
    
    print('Duplicate rows:',DF[DF.duplicated()].shape[0])
    
    print('Memory usage:',MemoryStr)
    
    print('##################################################')
    
    print('## Variable type ##')
    
    print('Categorical:', DF.select_dtypes(include = ['object']).shape[1])
    
    print('Numerical:',DF.select_dtypes(include = ['int64','int32','float32','float64']).shape[1])
    
    print('datetime:',DF.select_dtypes(include = ['datetime64[ns]']).shape[1])
    
    print('##################################################')
    
    print('## Null Data ##')
    
    print(NullDF.loc[NullDF['NullRatio'] > 0])
    
    print('##################################################')
    
    print('## Zero Data ##')
    
    print(ZeroDF.loc[ZeroDF['ZeroRatio'] > 0])
    
    print('##################################################')

def get_numerical_feature(DF,NumericalDivide = 20, AddNumericalCols = []):
    
    NumericalFeatures = list(DF.select_dtypes(include = ['int64','int32','float32','float64']).columns)
    
    for CheckFeature in list(DF.select_dtypes(include = ['int64','int32','float32','float64']).columns):

        if DF[CheckFeature].nunique() < NumericalDivide and CheckFeature not in AddNumericalCols:

            NumericalFeatures.remove(CheckFeature)
            
    return NumericalFeatures
    
    
def VariablesInfo(DF, Method ,Label = None,targetcols = None, NumericalDivide = 20,AddNumericalCols = []):
    
    NumericalFeatures = list(DF.select_dtypes(include = ['int64','int32','float32','float64']).columns)
    
    for CheckFeature in list(DF.select_dtypes(include = ['int64','int32','float32','float64']).columns):

        if DF[CheckFeature].nunique() < NumericalDivide and CheckFeature not in AddNumericalCols:

            NumericalFeatures.remove(CheckFeature)

    for Feature in DF.columns:
                    
        if Feature in NumericalFeatures:

            print('')
            print('#########################'+Feature+'#########################')
            
            print('Missing:',DF[DF[Feature].isnull() == True].shape[0],'('+str(round(DF[DF[Feature].isnull() == True].shape[0] / DF.shape[0] * 100,2))+'%)')
            
            print('Zeros:', DF.loc[DF[Feature] == 0,].shape[0], '('+str(round(DF.loc[DF[Feature] == 0].shape[0] / DF.shape[0] * 100,2))+'%)')
            
            print('Infinite:', DF.loc[(DF[Feature] == np.inf) | (DF[Feature] == -np.inf)].shape[0], '('+str(round(DF.loc[(DF[Feature] == np.inf) | (DF[Feature] == -np.inf)].shape[0] / DF.shape[0] * 100,2))+'%)')
            
            print('-------------------------------------------------------------')
            
            print('## Statistics ##')
            
            StatisticsName1 = pd.DataFrame(['Minimum:','Q1:','Median:','Q3:','Maximum:','IQR:','Q1-1.5IQR:','Q1+1.5IQR:'], columns = ['StatisticsName'])
            
            StatisticsName2 = pd.DataFrame(['Mean:','Standard deviation:','Coefficient of variation:','Kurtosis:','Skewness:','Variance:','Median Absolute Deviation:','Sum:'], columns = ['StatisticsName'])
            
            Minimum = DF[Feature].min()
            
            Q1 = DF[Feature].quantile(0.25)
            
            Median = DF[Feature].median()
            
            Q3 = DF[Feature].quantile(0.75)
            
            Maximum = DF[Feature].max()
            
            IQR = Q3 - Q1
            
            UnderLine = Q1 - 1.5 * IQR
            
            TopLine = Q3 + 1.5 * IQR

            Mean = DF[Feature].mean()
            
            STD = DF[Feature].std()
            
            CV = DF[Feature].std() / DF[Feature].mean()
            
            Kurtosis = DF[Feature].kurtosis()
            
            Skewness = DF[Feature].skew()
            
            Variance = DF[Feature].var()
            
            MAD = DF[Feature].mad()
            
            Sum = DF[Feature].sum()
            
            StatisticsValue1 = pd.DataFrame([Minimum,Q1,Median,Q3,Maximum,IQR,UnderLine,TopLine], columns = ['Value'])
            
            StatisticsValue2 = pd.DataFrame([Mean,STD,CV,Kurtosis,Skewness,Variance,MAD,Sum], columns = ['Value'])
            
            StatisticsDF = pd.DataFrame()
            
            StatisticsDF = pd.concat([StatisticsDF, StatisticsName1], axis = 1)
            StatisticsDF = pd.concat([StatisticsDF, StatisticsValue1], axis = 1)
            StatisticsDF = pd.concat([StatisticsDF, StatisticsName2], axis = 1)
            StatisticsDF = pd.concat([StatisticsDF, StatisticsValue2], axis = 1)
            
            print(StatisticsDF)
#            try:  
            PlotNumerical(DF,targetcols = [Feature], kde = True)
#            except:
#                PlotNumerical(DF,targetcols = [Feature], kde = False)
            
            
            if Label != None and Label != Feature:
                
                if Method == 'classifier':
#                    try:    
                    Plotkde(DF, targetcols = [Feature], classcols = [Label])
#                    except:
#                        Plotkde(DF, targetcols = [Feature], classcols = [Label],)
                elif Method == 'regression':
                    
                    PlotScatter(DF,labelcols = [Label] ,targetcols = [Feature] )
                    
                else:
                    print('Method just in [classifier, regression] ')
                    
        else:
            
            print('')
            print('#########################'+Feature+'#########################')         
            print('Distinct count',DF[Feature].nunique())         
            print('Missing:',DF[DF[Feature].isnull() == True].shape[0],'('+str(round(DF[DF[Feature].isnull() == True].shape[0] / DF.shape[0] * 100,2))+'%)')
            print('-------------------------------------------------------------')
            print('## Class Proportion ##')
            
            ClassDF = DF.groupby(Feature)[[Feature]].count()
            
            ClassDF.columns = ['Count']

            ClassDF['TotalData'] = DF.shape[0]

            ClassDF['Proportion'] = ClassDF['Count'] / ClassDF['TotalData']
            
            print(ClassDF)
            
            if DF[Feature].nunique() > 50:
                    print('Feature class too more')
            else:    
                PlotCategorical(DF, labelcols = None, targetcols = [Feature])
            
            if Label != None and Label != Feature:
                
                print('-------------------------------------------------------------')
                
                if Method == 'classifier':
                    print('## Label Ratio ##')
                    if DF[Feature].nunique() > 50:
                        print('Feature class too more')
                        
                    else:
                                
                        if DF[Label].nunique() > 2:
                                             
                            LabelDF = pd.get_dummies(DF[Label])
                             
                            LabelDF.columns = [Label + '_' + i for i in list(LabelDF.columns)]
                             
                            PlotDF = pd.concat([DF[Feature],LabelDF], axis = 1)
                             
                            PlotCategorical(PlotDF, labelcols = list(LabelDF.columns), targetcols = [Feature])
                             
                        else:
                            
                            PlotCategorical(DF, labelcols = [Label],targetcols = [Feature])
                            
                elif Method == 'regression':
                    print('## Label kde ##')
                    if DF[Feature].nunique() > 50:
                        print('Feature class too more')
                    else:
                        Plotkde(DF, targetcols = [Label], classcols = [Feature])
                else:
                    print('Method just in [classifier, regression] ')


def CorrelationInfo(DF,Label = None, NumericalDivide = 20,AddNumericalCols = [],figsize = (10,5), JustPlotLabel = False):
    
    print('#########################'+'Correlation'+'#########################')
    
    
    NumericalFeatures = get_numerical_feature(DF,NumericalDivide = NumericalDivide, AddNumericalCols = AddNumericalCols)
    
    if Label == None:
        pass
    else:
        NumericalFeatures.append(Label)
    
    for CorrMethod in ['pearson','spearman','kendall']:

        print('## '+CorrMethod+' ##')
    
        CorrResult = corr(DF[NumericalFeatures], targetcols = None, classcol = None,method = CorrMethod, plot = False, plot_gride = False)
            
        if JustPlotLabel == True:        
        
            print(CorrResult[[Label]])
            
            plt.figure(figsize = figsize)
                
            sns.heatmap(CorrResult[[Label]], cmap = plt.cm.RdYlBu_r, vmin = -0.25, annot = True, vmax = 0.6)
            
            plt.show()
        else:
            print(CorrResult)
            
            plt.figure(figsize = figsize)
                
            sns.heatmap(CorrResult, cmap = plt.cm.RdYlBu_r, vmin = -0.25, annot = True, vmax = 0.6)
            
            plt.show()
    
        print('-------------------------------------------------------------')
    

