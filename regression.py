import numpy as np
from scipy.optimize import curve_fit
import statsmodels.api as sm

def regressions(data):
    x = data
    y = [i for i in range(1, len(data))]
    
    # 线性拟合
    def linear_func(x, a, b):
        return a * x + b
    
    linear_params, linear_covariance = curve_fit(linear_func, x, y)
    linear_predicted = linear_func(x, *linear_params)
    linear_residuals = y - linear_predicted
    linear_sse = np.sum(linear_residuals ** 2)
    linear_sst = np.sum((y - np.mean(y)) ** 2)
    linear_r_squared = 1 - linear_sse / linear_sst
    linear_adj_r_squared = 1 - (1 - linear_r_squared) * (len(y) - 1) / (len(y) - 2)
    
    # 二次拟合
    def quadratic_func(x, a, b, c):
        return a * x ** 2 + b * x + c
    
    quadratic_params, quadratic_covariance = curve_fit(quadratic_func, x, y)
    quadratic_predicted = quadratic_func(x, *quadratic_params)
    quadratic_residuals = y - quadratic_predicted
    quadratic_sse = np.sum(quadratic_residuals ** 2)
    quadratic_sst = np.sum((y - np.mean(y)) ** 2)
    quadratic_r_squared = 1 - quadratic_sse / quadratic_sst
    quadratic_adj_r_squared = 1 - (1 - quadratic_r_squared) * (len(y) - 1) / (len(y) - 3)
    
    # 三次拟合
    def cubic_func(x, a, b, c, d):
        return a * x ** 3 + b * x ** 2 + c * x + d
    
    cubic_params, cubic_covariance = curve_fit(cubic_func, x, y)
    cubic_predicted = cubic_func(x, *cubic_params)
    cubic_residuals = y - cubic_predicted
    cubic_sse = np.sum(cubic_residuals ** 2)
    cubic_sst = np.sum((y - np.mean(y)) ** 2)
    cubic_r_squared = 1 - cubic_sse / cubic_sst
    cubic_adj_r_squared = 1 - (1 - cubic_r_squared) * (len(y) - 1) / (len(y) - 4)
    
    # 四次拟合
    def quartic_func(x, a, b, c, d, e):
        return a * x ** 4 + b * x ** 3 + c * x ** 2 + d * x + e
    
    quartic_params, quartic_covariance = curve_fit(quartic_func, x, y)
    quartic_predicted = quartic_func(x, *quartic_params)
    quartic_residuals = y - quartic_predicted
    quartic_sse = np.sum(quartic_residuals ** 2)
    quartic_sst = np.sum((y - np.mean(y)) ** 2)
    quartic_r_squared = 1 - quartic_sse / quartic_sst
    quartic_adj_r_squared = 1 - (1 - quartic_r_squared) * (len(y) - 1) / (len(y) - 5)
    
    return [[],[]]
    
    
    