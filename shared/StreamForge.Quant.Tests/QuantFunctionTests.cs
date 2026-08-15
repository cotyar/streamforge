using StreamForge.Engine;
using StreamForge.Engine.Sql;
using StreamForge.Quant;
using Xunit;

namespace StreamForge.Quant.Tests;

/// <summary>
/// Every assertion here is against a value known INDEPENDENTLY of this code — a textbook figure, or an
/// identity that has to hold whatever the implementation does (put-call parity, a par bond priced at
/// par, a par swap worth nothing, covered-interest parity). That is deliberate: a wrapper's bugs live
/// in argument order, units and sign conventions, and a test that recomputes the same formula would
/// agree with all of them.
/// </summary>
public class QuantFunctionTests
{
    private static readonly IReadOnlyList<IScalarFunction> Functions = QuantFunctions.All();

    private static double? Call(string name, params object?[] args)
    {
        var fn = Functions.First(f => f.Name == name);
        Assert.True(fn.IsValidArity(args.Length), $"{name} rejected {args.Length} arguments");
        return (double?)fn.Eval(args);
    }

    // Hull, Options Futures and Other Derivatives — the standard worked example.
    private const double S = 42, K = 40, T = 0.5, R = 0.10, Q = 0.0, Vol = 0.20;

    [Fact]
    public void Black_scholes_matches_the_textbook_example()
    {
        Assert.Equal(4.76, Call("BS_PRICE", S, K, T, R, Q, Vol, true)!.Value, 2);
        Assert.Equal(0.81, Call("BS_PRICE", S, K, T, R, Q, Vol, false)!.Value, 2);
    }

    /// <summary>C − P = S·e^(−qT) − K·e^(−rT). Holds for any model that is arbitrage-free, so it pins
    /// the discounting and the dividend term without re-deriving either.</summary>
    [Theory]
    [InlineData(0.0)]
    [InlineData(0.03)]
    public void Put_call_parity_holds(double q)
    {
        double call = Call("BS_PRICE", S, K, T, R, q, Vol, true)!.Value;
        double put = Call("BS_PRICE", S, K, T, R, q, Vol, false)!.Value;
        double expected = S * Math.Exp(-q * T) - K * Math.Exp(-R * T);
        Assert.Equal(expected, call - put, 8);
    }

    /// <summary>Delta_call − delta_put = e^(−qT), and gamma/vega are identical for the two — three
    /// relationships that catch a sign or scaling slip in the greeks without recomputing them.</summary>
    [Fact]
    public void The_greeks_satisfy_their_call_put_relationships()
    {
        const double q = 0.03;
        Assert.Equal(
            Math.Exp(-q * T),
            Call("BS_DELTA", S, K, T, R, q, Vol, true)!.Value - Call("BS_DELTA", S, K, T, R, q, Vol, false)!.Value,
            8);
        Assert.Equal(Call("BS_GAMMA", S, K, T, R, q, Vol, true)!.Value, Call("BS_GAMMA", S, K, T, R, q, Vol, false)!.Value, 10);
        Assert.Equal(Call("BS_VEGA", S, K, T, R, q, Vol, true)!.Value, Call("BS_VEGA", S, K, T, R, q, Vol, false)!.Value, 10);
    }

    /// <summary>Delta of this call is N(d1) = 0.779 and gamma is 0.0500 — hand-computed from the same
    /// textbook inputs, so they pin the absolute scale the relationships above cannot.</summary>
    [Fact]
    public void Delta_and_gamma_have_the_textbook_magnitudes()
    {
        Assert.Equal(0.779, Call("BS_DELTA", S, K, T, R, Q, Vol, true)!.Value, 3);
        Assert.Equal(0.0500, Call("BS_GAMMA", S, K, T, R, Q, Vol, true)!.Value, 4);
        // Vega is per unit of volatility, so one vol point is a hundredth of this. S·n(d1)·sqrt(T).
        Assert.Equal(8.813, Call("BS_VEGA", S, K, T, R, Q, Vol, true)!.Value, 2);
        // QuantLib's sign: a long option loses value as time passes.
        Assert.True(Call("BS_THETA", S, K, T, R, Q, Vol, true)!.Value < 0);
    }

    [Theory]
    [InlineData(0.0)]      // vol
    [InlineData(-0.2)]
    public void A_nonsensical_volatility_is_NULL_not_an_exception(double vol)
        => Assert.Null(Call("BS_PRICE", S, K, T, R, Q, vol, true));

    [Fact]
    public void NULL_and_non_numeric_arguments_give_NULL()
    {
        Assert.Null(Call("BS_PRICE", S, K, T, R, Q, null, true));
        Assert.Null(Call("BS_PRICE", S, K, T, R, Q, "loud", true));
        Assert.Null(Call("BS_PRICE", S, K, 0.0, R, Q, Vol, true));   // expired
        Assert.Null(Call("FX_FWD", -1.0, 0.01, 0.02, 1.0));           // negative spot
    }

    // ------------------------------------------------------------------
    // Bonds
    // ------------------------------------------------------------------

    /// <summary>A bond whose coupon equals its yield is worth par, whatever the maturity or frequency.</summary>
    [Theory]
    [InlineData(5.0, 1.0)]
    [InlineData(10.0, 2.0)]
    [InlineData(3.0, 4.0)]
    public void A_par_bond_prices_at_par(double years, double freq)
        => Assert.Equal(100.0, Call("BOND_PRICE", 100.0, 0.05, years, 0.05, freq)!.Value, 8);

    /// <summary>Modified duration of a zero-coupon bond is T/(1 + y/f) — the one closed form that does
    /// not depend on the coupon schedule at all.</summary>
    [Fact]
    public void Modified_duration_of_a_zero_is_its_maturity_discounted_once()
    {
        double actual = Call("BOND_DURATION", 100.0, 0.0, 10.0, 0.04, 2.0)!.Value;
        Assert.Equal(10.0 / (1 + 0.04 / 2), actual, 8);
    }

    /// <summary>DV01 is the value change for one basis point, so it has to agree with actually moving
    /// the yield by one — a finite-difference check the analytic formula cannot fake.</summary>
    [Fact]
    public void DV01_agrees_with_a_one_basis_point_reprice()
    {
        double dv01 = Call("BOND_DV01", 1_000_000.0, 0.05, 10.0, 0.04, 2.0)!.Value;
        double p0 = Call("BOND_PRICE", 1_000_000.0, 0.05, 10.0, 0.04, 2.0)!.Value;
        double p1 = Call("BOND_PRICE", 1_000_000.0, 0.05, 10.0, 0.0401, 2.0)!.Value;
        Assert.Equal(p0 - p1, dv01, 0);   // to the nearest currency unit on a million of notional
        Assert.True(dv01 > 0);
    }

    // ------------------------------------------------------------------
    // Swaps and FX
    // ------------------------------------------------------------------

    /// <summary>A swap struck at the curve's own rate is worth nothing — the definition of the par rate,
    /// and the sharpest single check on the annuity and the float-leg identity together.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_swap_struck_at_the_flat_rate_is_worth_nothing(bool payFixed)
        => Assert.Equal(0.0, Call("IRS_NPV", 1_000_000.0, 0.03, 5.0, 0.03, payFixed)!.Value, 6);

    [Fact]
    public void Pay_and_receive_fixed_are_exact_mirrors()
    {
        double pay = Call("IRS_NPV", 1_000_000.0, 0.02, 5.0, 0.03, true)!.Value;
        double receive = Call("IRS_NPV", 1_000_000.0, 0.02, 5.0, 0.03, false)!.Value;
        Assert.Equal(-pay, receive, 8);
        Assert.True(pay > 0, "paying 2% fixed when the curve is at 3% is worth something");
    }

    [Fact]
    public void Swap_DV01_agrees_with_a_one_basis_point_curve_bump()
    {
        double dv01 = Call("IRS_DV01", 1_000_000.0, 0.02, 5.0, 0.03, true)!.Value;
        double p0 = Call("IRS_NPV", 1_000_000.0, 0.02, 5.0, 0.03, true)!.Value;
        double p1 = Call("IRS_NPV", 1_000_000.0, 0.02, 5.0, 0.0301, true)!.Value;
        Assert.Equal(Math.Abs(p1 - p0), dv01, 8);
        Assert.True(dv01 > 0);
    }

    [Fact]
    public void FX_forward_is_covered_interest_parity()
    {
        Assert.Equal(1.25, Call("FX_FWD", 1.25, 0.02, 0.02, 3.0)!.Value, 10); // equal rates, no drift
        Assert.Equal(1.25 * Math.Exp((0.05 - 0.01) * 2.0), Call("FX_FWD", 1.25, 0.05, 0.01, 2.0)!.Value, 10);
    }

    // ------------------------------------------------------------------
    // Registration
    // ------------------------------------------------------------------

    [Fact]
    public void RegisterAll_puts_every_function_into_the_engine_registry()
    {
        try
        {
            QuantFunctions.RegisterAll();
            QuantFunctions.RegisterAll();   // idempotent — a host that registers twice is harmless

            var registered = SqlFunctions.RegisteredScalarNames();
            foreach (var fn in Functions)
            {
                Assert.Contains(fn.Name, registered);
                Assert.NotNull(SqlFunctions.FindScalar(fn.Name));
            }
            Assert.Equal(Functions.Count, registered.Count);
        }
        finally
        {
            SqlFunctions.Clear();
        }
    }

    /// <summary>End-to-end through the compiler: a registered function is a first-class scalar — typed,
    /// arity-checked and evaluated per row like any built-in.</summary>
    [Fact]
    public void A_quant_function_compiles_and_evaluates_in_real_SQL()
    {
        try
        {
            QuantFunctions.RegisterAll();
            var schema = new SourceSchema("book", new Dictionary<string, FieldKind>
            {
                ["spot"] = FieldKind.Double,
                ["strike"] = FieldKind.Double,
                ["vol"] = FieldKind.Double,
            });
            var schemas = new Dictionary<string, SourceSchema> { ["book"] = schema };

            var r = SqlCompiler.Compile(
                "SELECT BS_PRICE(spot, strike, 0.5, 0.1, 0.0, vol, TRUE) AS px FROM book", schemas);
            Assert.True(r.Ok, string.Join(";", r.Diagnostics.Select(d => d.Message)));
            Assert.Equal(FieldKind.Double, r.OutputSchema!.Fields["px"]);

            var rows = r.Plan!.CreateExecutor().OnEvent("book", new EventRecord(new Dictionary<string, object?>
            {
                ["_ts"] = 1000L, ["_source"] = "book",
                ["spot"] = 42.0, ["strike"] = 40.0, ["vol"] = 0.2,
            }));
            Assert.Equal(4.76, (double)Assert.Single(rows)["px"]!, 2);

            var bad = SqlCompiler.Compile("SELECT BS_PRICE(spot) AS px FROM book", schemas);
            Assert.False(bad.Ok);
            Assert.Contains(bad.Diagnostics, d => d.Message.Contains("wrong number of arguments"));
        }
        finally
        {
            SqlFunctions.Clear();
        }
    }
}
