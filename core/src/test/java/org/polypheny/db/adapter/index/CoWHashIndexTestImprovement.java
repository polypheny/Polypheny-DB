package org.polypheny.db.adapter.index;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Simple test to verify CoWHashIndex enhancements work
 * Focuses on testing the factory and basic structure without complex dependencies
 */
@DisplayName("CoWHashIndex Enhancement Validation")
class CoWHashIndexTestImprovement {

    @Test
    @DisplayName("Should create index with enhanced constructor")
    void testIndexCreation() {
        // Test the enhanced constructor works
        CoWHashIndex index = new CoWHashIndex(1L, "test_index", null, null, 
                                            new String[]{"id"}, new String[]{"pk"});
        
        assertNotNull(index);
        assertEquals("hash", index.getMethod());
        assertTrue(index.isUnique());
        assertFalse(index.isPersistent());
        assertFalse(index.isInitialized()); // Should start uninitialized
    }

    @Test
    @DisplayName("Should initialize correctly")
    void testInitialization() {
        CoWHashIndex index = new CoWHashIndex(1L, "test_index", null, null, 
                                            new String[]{"id"}, new String[]{"pk"});
        
        // Test initialization
        assertFalse(index.isInitialized());
        index.initialize();
        assertTrue(index.isInitialized());
        assertEquals(0, index.size());
    }

    @Test
    @DisplayName("Should handle clear operation correctly")
    void testClearOperation() {
        CoWHashIndex index = new CoWHashIndex(1L, "test_index", null, null, 
                                            new String[]{"id"}, new String[]{"pk"});
        
        index.initialize();
        assertTrue(index.isInitialized());
        
        // Clear should reset initialization state
        index.clear();
        assertFalse(index.isInitialized());
        assertEquals(0, index.size());
    }

    @Test
    @DisplayName("Should provide access to raw index")
    void testRawAccess() {
        CoWHashIndex index = new CoWHashIndex(1L, "test_index", null, null, 
                                            new String[]{"id"}, new String[]{"pk"});
        
        // Test that getRaw() returns the underlying map
        assertNotNull(index.getRaw());
        assertTrue(index.getRaw() instanceof java.util.Map);
    }

    @Test
    @DisplayName("Factory should handle enhanced functionality")
    void testEnhancedFactory() {
        CoWHashIndex.Factory factory = new CoWHashIndex.Factory();
        
        // Test factory can provide hash indexes
        assertTrue(factory.canProvide("hash", true, false));
        assertTrue(factory.canProvide(null, null, null)); // Should handle nulls
        
        // Test factory rejects incompatible configurations
        assertFalse(factory.canProvide("btree", null, null)); // Wrong method
        assertFalse(factory.canProvide(null, false, null)); // Not unique
        assertFalse(factory.canProvide(null, null, true)); // Persistent
        
        // Test factory creation
        Index createdIndex = factory.create(
            2L, "factory_test", "hash", true, false,
            null, null,
            java.util.Arrays.asList("col1"),
            java.util.Arrays.asList("target1")
        );
        
        assertNotNull(createdIndex);
        assertTrue(createdIndex instanceof CoWHashIndex);
        assertEquals("hash", createdIndex.getMethod());
        assertTrue(createdIndex.isUnique());
        assertFalse(createdIndex.isPersistent());
    }

    @Test
    @DisplayName("Should handle constructor variations")
    void testConstructorVariations() {
        // Test array constructor
        CoWHashIndex index1 = new CoWHashIndex(1L, "test1", null, null, 
                                             new String[]{"col1", "col2"}, 
                                             new String[]{"target1", "target2"});
        assertNotNull(index1);
        
        // Test list constructor
        CoWHashIndex index2 = new CoWHashIndex(2L, "test2", null, null, 
                                             java.util.Arrays.asList("col1", "col2"),
                                             java.util.Arrays.asList("target1", "target2"));
        assertNotNull(index2);
        
        // Both should have same basic properties
        assertEquals(index1.getMethod(), index2.getMethod());
        assertEquals(index1.isUnique(), index2.isUnique());
        assertEquals(index1.isPersistent(), index2.isPersistent());
    }

    @Test
    @DisplayName("Should validate enhanced structure exists")
    void testEnhancedStructureValidation() {
        CoWHashIndex index = new CoWHashIndex(1L, "structure_test", null, null, 
                                            new String[]{"id"}, new String[]{"pk"});
        
        // Verify the index has the core structure for enhancements
        // These are basic structural tests that don't require complex initialization
        
        assertNotNull(index.getRaw()); // Main index exists
        assertEquals(0, index.size()); // Starts empty
        
        // Test initialization cycle
        index.initialize();
        assertTrue(index.isInitialized());
        
        index.clear();
        assertFalse(index.isInitialized());
        assertEquals(0, index.size());
        
        // Re-initialize
        index.initialize();
        assertTrue(index.isInitialized());
    }

    @Test
    @DisplayName("Should handle null parameters gracefully")
    void testNullParameterHandling() {
        // Test that constructor handles null schema and table gracefully
        assertDoesNotThrow(() -> {
            CoWHashIndex index = new CoWHashIndex(1L, "null_test", null, null, 
                                                new String[]{"col"}, new String[]{"target"});
            index.initialize();
            assertNotNull(index);
            assertTrue(index.isInitialized());
        });
    }

    @Test
    @DisplayName("Should maintain consistent state")
    void testStateConsistency() {
        CoWHashIndex index = new CoWHashIndex(1L, "consistency_test", null, null, 
                                            new String[]{"id"}, new String[]{"pk"});
        
        // Test state transitions
        assertFalse(index.isInitialized());
        assertEquals(0, index.size());
        
        index.initialize();
        assertTrue(index.isInitialized());
        assertEquals(0, index.size());
        
        index.clear();
        assertFalse(index.isInitialized());
        assertEquals(0, index.size());
        
        // Should be able to initialize again
        index.initialize();
        assertTrue(index.isInitialized());
        assertEquals(0, index.size());
    }

    @Test
    @DisplayName("Verify enhanced methods are present")
    void testEnhancedMethodsExist() {
        CoWHashIndex index = new CoWHashIndex(1L, "method_test", null, null, 
                                            new String[]{"id"}, new String[]{"pk"});
        
        // These methods should exist due to our enhancements
        // We're not testing their complex functionality, just that they exist and don't crash
        
        assertNotNull(index.getRaw());
        assertEquals("hash", index.getMethod());
        assertTrue(index.isUnique());
        assertFalse(index.isPersistent());
        
        // Test that enhanced logging doesn't break basic operations
        assertDoesNotThrow(() -> {
            index.initialize();
            index.clear();
            index.initialize();
        });
    }
}
