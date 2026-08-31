using QLNet;
using StreamsForge.Engine;
using StreamsForge.Engine.Sql;

namespace StreamsForge.Quant;

/// <summary>
/// Pricing and risk scalars, registered into the Engine through <see cref="SqlFunctions"/> so the Engine
/// itself takes no pricing-library dependency.
///
/// <para><b>What is QuantLib-backed and what is not, and why.</b> The Black family goes through QLNet's
/// <see cref="BlackCalculator"/> — the same code QuantLib runs, and the place where a hand-rolled
/// wrapper actually gets things wrong (greek scaling, the dividend-yield term, put/call signs).
/// Critically it is <b>date-free</b>: it takes a forward, a standard deviation and a discount factor,
/// so it touches none of QLNet's <c>Settings.evaluationDate</c> global. QLNet's instrument layer (Bond,
/// VanillaSwap, Schedule) does depend on that thread-static evaluation date, which is flatly
/// incompatible with a scalar function that must be pure, total and safe to evaluate concurrently from
/// many rows — so the flat-curve bond and swap measures below are closed-form instead. They are
/// deliberately simple parameterisations (a single yield, a flat curve), which is exactly the shape a
/// closed form is right for; the moment a real term structure is wanted, that is a different function
/// signature and a different conversation.</para>
///
/// <para><b>Every function here is total.</b> A NULL argument, a non-numeric one, or a nonsensical
/// domain (vol ≤ 0, t ≤ 0, a negative price) yields NULL — never an exception. A throwing scalar would
/// kill the pipeline for every other row, which is the failure mode every built-in scalar in this
/// dialect is written to avoid, and registered functions are held to the same rule.</para>
/// </summary>
public static class QuantFunctions
{
    /// <summary>Call once from host startup, before any SQL is compiled — the same deadline
    /// <c>DatabaseConnectors.RegisterAll()</c> has. Idempotent.</summary>
    public static void RegisterAll()
    {
        foreach (var f in All()) SqlFunctions.Register(f);
    }

    public static IReadOnlyList<IScalarFunction> All() =>
    [
        new Fn("BS_PRICE", 7, a => Black(a, BlackMeasure.Price)),
        new Fn("BS_DELTA", 7, a => Black(a, BlackMeasure.Delta)),
        new Fn("BS_GAMMA", 7, a => Black(a, BlackMeasure.Gamma)),
        new Fn("BS_VEGA", 7, a => Black(a, BlackMeasure.Vega)),
        new Fn("BS_THETA", 7, a => Black(a, BlackMeasure.Theta)),
        new Fn("BOND_PRICE", 5, a => Bond(a, BondMeasure.Price)),
        new Fn("BOND_DV01", 5, a => Bond(a, BondMeasure.Dv01)),
        new Fn("BOND_DURATION", 5, a => Bond(a, BondMeasure.ModifiedDuration)),
        new Fn("IRS_NPV", 5, a => Swap(a, dv01: false)),
        new Fn("IRS_DV01", 5, a => Swap(a, dv01: true)),
        new Fn("FX_FWD", 4, FxForward),
    ];

    // ------------------------------------------------------------------
    // Black-Scholes-Merton — QLNet's own BlackCalculator
    // ------------------------------------------------------------------

    private enum BlackMeasure { Price, Delta, Gamma, Vega, Theta }

    /// <summary>(spot, strike, t_years, r, q, vol, is_call). Continuous rates; <c>is_call</c> is a bool
    /// or any nonzero number.</summary>
    private static double? Black(IReadOnlyList<double> a, BlackMeasure measure)
    {
        double spot = a[0], strike = a[1], t = a[2], r = a[3], q = a[4], vol = a[5];
        bool isCall = a[6] != 0;
        if (spot <= 0 || strike <= 0 || t <= 0 || vol <= 0) return null;

        // The three date-free inputs BlackCalculator wants. Carrying the dividend yield in the forward
        // (rather than through a term structure) is what keeps this out of QLNet's global evaluation date.
        double forward = spot * Math.Exp((r - q) * t);
        double stdDev = vol * Math.Sqrt(t);
        double discount = Math.Exp(-r * t);

        var payoff = new PlainVanillaPayoff(isCall ? Option.Type.Call : Option.Type.Put, strike);
        var black = new BlackCalculator(payoff, forward, stdDev, discount);

        return measure switch
        {
            BlackMeasure.Price => black.value(),
            BlackMeasure.Delta => black.delta(spot),
            BlackMeasure.Gamma => black.gamma(spot),
            // Per unit of volatility (0.01 of vega = one vol point), matching QuantLib.
            BlackMeasure.Vega => black.vega(t),
            // Per year, and negative for a long option — QuantLib's sign, not the "decay is positive"
            // convention some risk systems print.
            BlackMeasure.Theta => black.theta(spot, t),
            _ => null,
        };
    }

    // ------------------------------------------------------------------
    // Flat-yield bond — closed form, see the class doc
    // ------------------------------------------------------------------

    private enum BondMeasure { Price, Dv01, ModifiedDuration }

    /// <summary>(face, coupon, years, yield, freq). <c>coupon</c> and <c>yield</c> are annual decimals;
    /// <c>freq</c> is coupons per year. Duration is MODIFIED duration, because that is the one DV01 is
    /// built from and the one a risk system means by the bare word.</summary>
    private static double? Bond(IReadOnlyList<double> a, BondMeasure measure)
    {
        double face = a[0], coupon = a[1], years = a[2], yield = a[3], freq = a[4];
        if (face <= 0 || years <= 0 || freq <= 0 || coupon < 0) return null;

        long periods = (long)Math.Round(years * freq);
        // ponytail: a plain loop over coupons, so the period count is bounded rather than trusted.
        // 12000 is a thousand years of monthly coupons; past that this is a typo, not a bond.
        if (periods < 1 || periods > 12_000) return null;

        double periodYield = yield / freq;
        if (periodYield <= -1) return null;

        double periodCoupon = face * coupon / freq;
        double price = 0, weightedTime = 0;
        for (long i = 1; i <= periods; i++)
        {
            double cash = periodCoupon + (i == periods ? face : 0);
            double pv = cash / Math.Pow(1 + periodYield, i);
            price += pv;
            weightedTime += pv * (i / freq);
        }
        if (price <= 0) return null;

        double macaulay = weightedTime / price;
        double modified = macaulay / (1 + periodYield);

        return measure switch
        {
            BondMeasure.Price => price,
            BondMeasure.ModifiedDuration => modified,
            // The market convention: value change for one basis point, reported positive.
            BondMeasure.Dv01 => modified * price * 0.0001,
            _ => null,
        };
    }

    // ------------------------------------------------------------------
    // Flat-curve vanilla IRS — closed form, see the class doc
    // ------------------------------------------------------------------

    /// <summary>(notional, fixed_rate, years, flat_rate, pay_fixed). Annual payments discounted on a
    /// flat annually-compounded curve, so the float leg is worth <c>notional * (1 - df_n)</c> — the
    /// standard par-float-leg identity, which is what makes a swap struck at the flat rate worth zero.</summary>
    private static double? Swap(IReadOnlyList<double> a, bool dv01)
    {
        if (dv01)
        {
            // A one-basis-point bump of the curve, valued the same way. Doing it by revaluation rather
            // than analytically keeps DV01 consistent with IRS_NPV by construction — if the NPV formula
            // ever changes, its sensitivity changes with it.
            double[] bumped = [a[0], a[1], a[2], a[3] + 0.0001, a[4]];
            double? baseNpv = Swap(a, dv01: false);
            double? bumpedNpv = Swap(bumped, dv01: false);
            return baseNpv is { } b && bumpedNpv is { } u ? Math.Abs(u - b) : null;
        }

        double notional = a[0], fixedRate = a[1], years = a[2], flat = a[3];
        bool payFixed = a[4] != 0;
        if (notional <= 0 || years <= 0 || flat <= -1) return null;

        long periods = (long)Math.Round(years);
        if (periods < 1 || periods > 200) return null;

        double annuity = 0;
        for (long i = 1; i <= periods; i++) annuity += 1.0 / Math.Pow(1 + flat, i);
        double finalDiscount = 1.0 / Math.Pow(1 + flat, periods);

        double fixedLeg = notional * fixedRate * annuity;
        double floatLeg = notional * (1 - finalDiscount);
        return payFixed ? floatLeg - fixedLeg : fixedLeg - floatLeg;
    }

    /// <summary>(spot, r_dom, r_for, t_years) — covered-interest parity on continuous rates.</summary>
    private static double? FxForward(IReadOnlyList<double> a)
    {
        double spot = a[0], rDom = a[1], rFor = a[2], t = a[3];
        if (spot <= 0 || t < 0) return null;
        return spot * Math.Exp((rDom - rFor) * t);
    }

    // ------------------------------------------------------------------

    /// <summary>Every function here has a fixed arity, takes numbers and returns a number or NULL, so
    /// one adapter covers all of them: it does the NULL/non-numeric screening once and hands the body a
    /// plain double list. A body returning null (or a NaN/Infinity that slipped out of a degenerate
    /// input) becomes SQL NULL.</summary>
    private sealed class Fn(string name, int arity, Func<IReadOnlyList<double>, double?> body) : IScalarFunction
    {
        public string Name => name;

        public bool IsValidArity(int argCount) => argCount == arity;

        public FieldKind? ResultKind(IReadOnlyList<FieldKind?> argKinds) => FieldKind.Double;

        public object? Eval(IReadOnlyList<object?> args)
        {
            if (args.Count != arity) return null;
            var numbers = new double[arity];
            for (int i = 0; i < arity; i++)
            {
                if (!TryNumber(args[i], out numbers[i])) return null;
            }

            double? result;
            try
            {
                result = body(numbers);
            }
            catch (Exception)
            {
                // QLNet throws on inputs its own validation rejects. The domain guards above catch the
                // ones worth naming, but a library exception must never reach the row loop: a scalar
                // that throws takes down the pipeline for every other row too.
                return null;
            }

            return result is { } d && double.IsFinite(d) ? d : null;
        }

        /// <summary>Bools are accepted for the flag-shaped arguments (is_call, pay_fixed) because that
        /// is what a Bool column or a comparison produces; longs and doubles for the rest.</summary>
        private static bool TryNumber(object? value, out double number)
        {
            switch (value)
            {
                case double d: number = d; return true;
                case long l: number = l; return true;
                case bool b: number = b ? 1 : 0; return true;
                default: number = 0; return false;
            }
        }
    }
}
