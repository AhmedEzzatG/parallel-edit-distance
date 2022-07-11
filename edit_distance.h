//
// Created by Ahmed Ezzat on 27/06/2022.
//

#ifndef CMAKE_BUILD_DEBUG_EDIT_DISTANCE_H
#define CMAKE_BUILD_DEBUG_EDIT_DISTANCE_H

#include <string.h>

int _3min(int a, int b, int c) {
    if (a <= b && a <= c)
        return a;
    if (b <= a && b <= c)
        return b;
    return c;
}


int edit_distance(char *a, char *b) {
    int n = strlen(a), m = strlen(b);
    int **dp = malloc((n + 1) * sizeof(int *));
    for (int i = 0; i <= n; i++)
        dp[i] = malloc((m + 1) * sizeof(int));
    for (int i = 0; i <= n; i++)
        for (int j = 0; j <= m; j++)
            dp[i][j] = 0;
    for (int i = n; i >= 0; i--) {
        for (int j = m; j >= 0; j--) {
            if (i == n && j == m)continue;
            if (i == n)dp[i][j] = 1 + dp[i][j + 1];
            else if (j == m)dp[i][j] = 1 + dp[i + 1][j];
            else if (a[i] == b[j])dp[i][j] = dp[i + 1][j + 1];
            else dp[i][j] = 1 + _3min(dp[i + 1][j], dp[i][j + 1], dp[i + 1][j + 1]);
        }
    }
    int val = dp[0][0];
    return val;
}


#endif //CMAKE_BUILD_DEBUG_EDIT_DISTANCE_H
