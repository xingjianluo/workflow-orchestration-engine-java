package com.github.xingjianluo.workfloworchestrationenginejava.tests;

import com.github.xingjianluo.workfloworchestrationenginejava.engine.DefinitionValidator;
import com.github.xingjianluo.workfloworchestrationenginejava.error.ErrorCode;
import com.github.xingjianluo.workfloworchestrationenginejava.error.WorkflowException;
import com.github.xingjianluo.workfloworchestrationenginejava.model.WorkflowDefinition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class DefinitionValidationTest {

    private final DefinitionValidator validator = new DefinitionValidator();

    private WorkflowDefinition def(String id, Map<String, List<String>> deps) {
        return new WorkflowDefinition(id, 3, 10L, deps);
    }

    @Test
    void validDefinitionPasses() {
        validator.validate(def("ok", Map.of(
                "a", List.of(),
                "b", List.of("a"),
                "c", List.of("a", "b"))));
    }

    @Test
    void rejectsBlankRequestId() {
        WorkflowException ex = assertThrows(WorkflowException.class,
                () -> validator.validate(def("  ", Map.of("a", List.of()))));
        assertEquals(ErrorCode.INVALID_DEFINITION, ex.getErrorCode());
    }

    @Test
    void rejectsInvalidMaxAttempts() {
        WorkflowException ex = assertThrows(WorkflowException.class,
                () -> validator.validate(new WorkflowDefinition("x", 0, 0L, Map.of("a", List.of()))));
        assertEquals(ErrorCode.INVALID_DEFINITION, ex.getErrorCode());
    }

    @Test
    void rejectsDuplicateDependency() {
        WorkflowException ex = assertThrows(WorkflowException.class,
                () -> validator.validate(def("dup", Map.of("a", List.of(), "b", List.of("a", "a")))));
        assertEquals(ErrorCode.DUPLICATE_NODE, ex.getErrorCode());
    }

    @Test
    void rejectsMissingDependency() {
        WorkflowException ex = assertThrows(WorkflowException.class,
                () -> validator.validate(def("missing", Map.of("a", List.of("ghost")))));
        assertEquals(ErrorCode.MISSING_DEPENDENCY, ex.getErrorCode());
    }

    @Test
    void rejectsDirectCycle() {
        WorkflowException ex = assertThrows(WorkflowException.class,
                () -> validator.validate(def("cyc", Map.of("a", List.of("b"), "b", List.of("a")))));
        assertEquals(ErrorCode.CYCLE_DETECTED, ex.getErrorCode());
    }

    @Test
    void rejectsLongCycle() {
        WorkflowException ex = assertThrows(WorkflowException.class,
                () -> validator.validate(def("cyc2", Map.of(
                        "a", List.of("c"),
                        "b", List.of("a"),
                        "c", List.of("b")))));
        assertEquals(ErrorCode.CYCLE_DETECTED, ex.getErrorCode());
    }

    @Test
    void selfLoopIsCycle() {
        WorkflowException ex = assertThrows(WorkflowException.class,
                () -> validator.validate(def("self", Map.of("a", List.of("a")))));
        assertEquals(ErrorCode.CYCLE_DETECTED, ex.getErrorCode());
    }

    @Test
    void errorCodesAreDistinguishable() {
        try {
            validator.validate(def("m", Map.of("a", List.of("nope"))));
            fail();
        } catch (WorkflowException e) {
            assertEquals(ErrorCode.MISSING_DEPENDENCY, e.getErrorCode());
        }
        try {
            validator.validate(def("c", Map.of("a", List.of("a"))));
            fail();
        } catch (WorkflowException e) {
            assertEquals(ErrorCode.CYCLE_DETECTED, e.getErrorCode());
        }
    }
}
