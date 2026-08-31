using StreamsForge.AppCore.Connectors.Polling;
using Xunit;

namespace StreamsForge.Host.Tests;

public class DedupTrackerTests
{
    [Fact]
    public void Seen_A_brand_new_key_returns_false_and_records_it()
    {
        var tracker = new DedupTracker();

        Assert.False(tracker.Seen("k1"));
        Assert.Equal(1, tracker.Count);
    }

    [Fact]
    public void Seen_A_repeated_key_returns_true_and_does_not_grow_the_count()
    {
        var tracker = new DedupTracker();
        tracker.Seen("k1");

        Assert.True(tracker.Seen("k1"));
        Assert.Equal(1, tracker.Count);
    }

    [Fact]
    public void Seen_Distinct_keys_are_all_tracked_independently()
    {
        var tracker = new DedupTracker();

        Assert.False(tracker.Seen("a"));
        Assert.False(tracker.Seen("b"));
        Assert.True(tracker.Seen("a"));
        Assert.True(tracker.Seen("b"));
        Assert.Equal(2, tracker.Count);
    }

    [Fact]
    public void FIFO_eviction_kicks_in_exactly_at_the_10000_key_bound()
    {
        var tracker = new DedupTracker();

        for (var i = 0; i < DedupTracker.MaxKeys; i++)
            tracker.Seen($"k{i}");

        Assert.Equal(DedupTracker.MaxKeys, tracker.Count);

        // One more key pushes past the bound — the oldest ("k0") is evicted.
        tracker.Seen($"k{DedupTracker.MaxKeys}");

        Assert.Equal(DedupTracker.MaxKeys, tracker.Count); // still bounded
        Assert.False(tracker.Seen("k0"));                  // forgotten — treated as new again
    }

    [Fact]
    public void FIFO_eviction_forgets_oldest_first_not_newest()
    {
        var tracker = new DedupTracker();

        for (var i = 0; i < DedupTracker.MaxKeys; i++)
            tracker.Seen($"k{i}");

        tracker.Seen($"k{DedupTracker.MaxKeys}"); // evicts k0

        // The second-oldest key is still remembered.
        Assert.True(tracker.Seen("k1"));
    }

    [Fact]
    public void ToPersistable_Preserves_insertion_order_oldest_first()
    {
        var tracker = new DedupTracker();
        tracker.Seen("first");
        tracker.Seen("second");
        tracker.Seen("third");

        Assert.Equal(["first", "second", "third"], tracker.ToPersistable());
    }

    [Fact]
    public void Persist_restore_round_trip_preserves_seen_state_and_count()
    {
        var original = new DedupTracker();
        original.Seen("a");
        original.Seen("b");
        original.Seen("c");

        var restored = new DedupTracker(original.ToPersistable());

        Assert.Equal(original.Count, restored.Count);
        Assert.True(restored.Seen("a"));
        Assert.True(restored.Seen("b"));
        Assert.True(restored.Seen("c"));
        Assert.False(restored.Seen("d")); // still recognizes genuinely-new keys
    }

    [Fact]
    public void Persist_restore_round_trip_preserves_fifo_order_for_future_eviction()
    {
        var original = new DedupTracker();
        for (var i = 0; i < DedupTracker.MaxKeys; i++)
            original.Seen($"k{i}");

        var restored = new DedupTracker(original.ToPersistable());
        restored.Seen($"k{DedupTracker.MaxKeys}"); // should evict k0, the oldest

        // Check k1 (still present) BEFORE k0 — asserting Seen("k0") itself re-records k0 (it was
        // evicted, so it now looks brand new), which would otherwise evict k1 in turn and make
        // the order of these two assertions matter for the wrong reason.
        Assert.True(restored.Seen("k1"));
        Assert.False(restored.Seen("k0"));
    }

    [Fact]
    public void Null_persisted_list_starts_empty()
    {
        var tracker = new DedupTracker(null);
        Assert.Equal(0, tracker.Count);
        Assert.Empty(tracker.ToPersistable());
    }
}

public class FileLedgerTests
{
    [Fact]
    public void IsNewOrChanged_Unknown_file_is_true()
    {
        var ledger = new FileLedger();
        Assert.True(ledger.IsNewOrChanged("a.json", 1000));
    }

    [Fact]
    public void IsNewOrChanged_Same_mtime_after_recording_is_false()
    {
        var ledger = new FileLedger();
        ledger.Record("a.json", 1000);

        Assert.False(ledger.IsNewOrChanged("a.json", 1000));
    }

    [Fact]
    public void IsNewOrChanged_Different_mtime_after_recording_is_true()
    {
        var ledger = new FileLedger();
        ledger.Record("a.json", 1000);

        Assert.True(ledger.IsNewOrChanged("a.json", 2000));
    }

    [Fact]
    public void IsNewOrChanged_Does_not_itself_record_repeated_calls_stay_true()
    {
        var ledger = new FileLedger();

        Assert.True(ledger.IsNewOrChanged("a.json", 1000));
        Assert.True(ledger.IsNewOrChanged("a.json", 1000)); // no Record() call in between
    }

    [Fact]
    public void Record_Updating_an_existing_name_does_not_change_the_ledger_size()
    {
        var ledger = new FileLedger();
        ledger.Record("a.json", 1000);
        ledger.Record("a.json", 2000); // same name, new mtime

        Assert.Single(ledger.ToPersistable());
        Assert.Equal(2000, ledger.ToPersistable()["a.json"]);
    }

    [Fact]
    public void FIFO_eviction_kicks_in_exactly_at_the_10000_entry_bound()
    {
        var ledger = new FileLedger();

        for (var i = 0; i < FileLedger.MaxEntries; i++)
            ledger.Record($"f{i}.json", i);

        Assert.Equal(FileLedger.MaxEntries, ledger.ToPersistable().Count);

        ledger.Record($"f{FileLedger.MaxEntries}.json", FileLedger.MaxEntries); // evicts f0

        Assert.Equal(FileLedger.MaxEntries, ledger.ToPersistable().Count);
        Assert.True(ledger.IsNewOrChanged("f0.json", 0)); // forgotten — looks brand new again
    }

    [Fact]
    public void FIFO_eviction_forgets_oldest_first_not_newest()
    {
        var ledger = new FileLedger();

        for (var i = 0; i < FileLedger.MaxEntries; i++)
            ledger.Record($"f{i}.json", i);

        ledger.Record($"f{FileLedger.MaxEntries}.json", FileLedger.MaxEntries); // evicts f0

        // f1 (second-oldest) is still remembered with its original mtime.
        Assert.False(ledger.IsNewOrChanged("f1.json", 1));
    }

    [Fact]
    public void Re_recording_an_existing_name_does_not_reset_its_fifo_position()
    {
        var ledger = new FileLedger();

        for (var i = 0; i < FileLedger.MaxEntries; i++)
            ledger.Record($"f{i}.json", i);

        // Touch the oldest entry again with a new mtime — per the documented contract this does
        // NOT move it to the back of the FIFO order.
        ledger.Record("f0.json", 999);

        // One more brand-new file should still evict f0 (still oldest by first-record order),
        // not f1.
        ledger.Record($"f{FileLedger.MaxEntries}.json", FileLedger.MaxEntries);

        Assert.True(ledger.IsNewOrChanged("f0.json", 999));  // evicted despite the touch
        Assert.False(ledger.IsNewOrChanged("f1.json", 1));   // still present
    }

    [Fact]
    public void Persist_restore_round_trip_preserves_recorded_state()
    {
        var original = new FileLedger();
        original.Record("a.json", 1000);
        original.Record("b.json", 2000);

        var restored = new FileLedger(original.ToPersistable());

        Assert.False(restored.IsNewOrChanged("a.json", 1000));
        Assert.False(restored.IsNewOrChanged("b.json", 2000));
        Assert.True(restored.IsNewOrChanged("c.json", 3000));
    }

    [Fact]
    public void Null_persisted_map_starts_empty()
    {
        var ledger = new FileLedger(null);
        Assert.Empty(ledger.ToPersistable());
        Assert.True(ledger.IsNewOrChanged("anything.json", 0));
    }
}
