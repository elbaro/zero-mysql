use crate::constant::{
    CAPABILITIES_ALWAYS_DISABLED, CAPABILITIES_ALWAYS_ENABLED, CAPABILITIES_CONFIGURABLE,
    CapabilityFlags,
};

#[test]
fn test_capability_flags_classification() {
    // All 32 bits set (complete set of possible flags)
    const ALL_FLAGS: u32 = 0xFFFFFFFF;

    let always_enabled = CAPABILITIES_ALWAYS_ENABLED;
    let configurable = CAPABILITIES_CONFIGURABLE;
    let always_disabled = CAPABILITIES_ALWAYS_DISABLED;

    // Test 1: No overlap between categories
    assert!(
        always_enabled.intersection(configurable).is_empty(),
        "ALWAYS_ENABLED and CONFIGURABLE must not overlap"
    );
    assert!(
        always_enabled.intersection(always_disabled).is_empty(),
        "ALWAYS_ENABLED and ALWAYS_DISABLED must not overlap"
    );
    assert!(
        configurable.intersection(always_disabled).is_empty(),
        "CONFIGURABLE and ALWAYS_DISABLED must not overlap"
    );

    // Test 2: Union covers all flags
    let union = always_enabled | configurable | always_disabled;
    assert_eq!(
        union.bits(),
        ALL_FLAGS,
        "Union of all three categories must equal all possible flags (0xFFFFFFFF). Missing flags: 0x{:08X}",
        ALL_FLAGS & !union.bits()
    );

    // Test 3: Verify specific critical flags are in correct categories
    assert!(
        always_enabled.contains(CapabilityFlags::CLIENT_PROTOCOL_41),
        "CLIENT_PROTOCOL_41 must be always enabled"
    );
    assert!(
        always_enabled.contains(CapabilityFlags::CLIENT_PLUGIN_AUTH),
        "CLIENT_PLUGIN_AUTH must be always enabled"
    );
    assert!(
        always_disabled.contains(CapabilityFlags::CLIENT_INTERACTIVE),
        "CLIENT_INTERACTIVE must be always disabled (we're not interactive)"
    );
}
