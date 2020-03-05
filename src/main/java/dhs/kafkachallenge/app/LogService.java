package dhs.kafkachallenge.app;

import dhs.kafkachallenge.streams.LogEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
public class LogService {

    @GetMapping("/logs/{recordId}")
    public List<LogEvent> getLogs(@PathVariable("recordId") String recordId) {

    List<LogEvent> logEvents = new ArrayList();

    return logEvents;
    }
}
