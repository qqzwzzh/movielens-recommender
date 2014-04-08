Recommender in MovieLens dataset using UV Decomposition
=====================

This is a maven application to recommend movies to users based on their ratings and other user ratings using collaborative filtering and UV decomposition or product of factors. This application uses batch gradient descent.

The procedure followed is as per the text book - Mining Massive Datasets by Anand Rajaraman et al.

This implementaion is incomplete in terms of recommending. It computes UV matrices from the given input. Also, it computes UV for a predefined no. of iterations rather than using RMSE which should actually be used. Also, the final predictions after computing UV is not implemented.

I also referred following for implementing this.
1. https://github.com/yebrahim/UV_decomposer_hadoop
2. https://github.com/akbindal/NetFlix-Recommender
