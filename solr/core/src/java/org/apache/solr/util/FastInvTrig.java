
//Copyright (c) 2021, Dan Rosher
//    All rights reserved.
//
//    This source code is licensed under the BSD-style license found in the
//    LICENSE file in the root directory of this source tree.

package org.apache.solr.util;

public class FastInvTrig {

    final static double pip2 = Math.PI / 2.0;
    static final double SQRT2 = 1.0 / Math.sqrt(2.0);

    final static double[] TABLE;
    final static int MAX_TERMS = 100;
    final static int DEF_TERMS = 10;

    static {
        TABLE = new double[MAX_TERMS];
        double factor = 1.0;
        double divisor = 1.0;
        for (int n = 0; n < MAX_TERMS; n++) {
            TABLE[n] = factor / divisor;
            divisor += 2;
            factor *= (2 * n + 1.0) / ((n + 1) * 2);
        }
    }

    private static double asin2(double x, int n_terms) {
        if (n_terms > MAX_TERMS)
            throw new IllegalArgumentException("Too many terms");
        double acc = x;
        double tempExp = x;
        double x2 = x * x;
        for (int n = 1; n < n_terms; n++) {
            tempExp *= x2;
            acc += TABLE[n] * tempExp;
        }
        return acc;
    }

    // split domain for faster convergence i.e. fewer maclaurin terms required
    // see https://www.wolframalpha.com/input/?i=arcsin%28sqrt%281-x%5E2%29%29-acos%28x%29 for x > 0
    // arcsin(sqrt(1-x^2)) = acos(x) for x > 0
    // arcsin(sqrt(1-x^2)) = acos(x) = pi/2 - arcsin(x)  for x > 0
    // arcsin(sqrt(1-x^2)) = pi/2 - arcsin(x)  for x > 0
    // arcsin(x) = pi/2 - arcsin(sqrt(1-x^2)) for x > 0 .... 1
    //
    // see https://www.wolframalpha.com/input/?i=arcsin%28x%29+-+arcsin%28sqrt%281-x%5E2%29%29+%2B+pi%2F2+ for x < 0
    // arcsin(sqrt(1-x^2)) = pi-acos(x) for x < 0
    // arcsin(sqrt(1-x^2)) = pi-(pi/2 - arcsin(x)) for x < 0
    // arcsin(sqrt(1-x^2)) = pi/2 + arcsin(x)  for x < 0
    // arcsin(x) = arcsin(sqrt(1-x^2)) - pi/2  for x < 0 .... 2

    // within domain [-SQRT2 <= x <=  SQRT2] use arcsin(x). outside this range use formulae above.
    // so that convergence is faster
    //This way we can transform input x into [-1/sqrt(2),1/sqrt(2)], where convergence is relatively fast.

    public static double asin(double x, int n_terms) {
        if (x > SQRT2)
            return pip2 - asin2(Math.sqrt(1 - (x * x)), n_terms);
        else if (Math.abs(x) <= SQRT2)
            return asin2(x, n_terms);
        return asin2(Math.sqrt(1 - (x * x)), n_terms) - pip2;
    }

    public static double asin(double x) {
        return asin(x, DEF_TERMS);
    }

    public static double acos(double x, int n_terms) {
        return Math.abs(x) <= SQRT2 ? pip2 - asin2(x, n_terms) : asin2(Math.sqrt(1 - (x * x)), n_terms);
    }

    public static double acos(double x) {
        return acos(x, DEF_TERMS);
    }

    //Following for completion for Inverse trigonometric functions
    public static double atan(double x) {
        return asin(x / Math.sqrt(1 + x * x));
    }

    public static double acot(double x) {
        return pip2 - atan(x);
    }

    public static double asec(double x) {
        return acos(1 / x);
    }

    public static double acsc(double x) {
        return pip2 - asec(x);
    }

}
