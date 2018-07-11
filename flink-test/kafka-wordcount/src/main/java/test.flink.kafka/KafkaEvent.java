package test.flink.kafka;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaEvent {
    private String word;
    private int frequency;
    private long timestamp;
    
    public KafkaEvent() {}
    
    public KafkaEvent(String word, int frequency, long timestamp) {
        this.word = word;
        this.frequency = frequency;
        this.timestamp = timestamp;
    }
    
    public static KafkaEvent empty() {
        KafkaEvent event = new KafkaEvent();
        event.setWord("[EMPTY]");
        event.setFrequency(0);
        event.setTimestamp(0);
        return event;
    }
    
    public String getWord() {
        return word;
    }
    
    public void setWord(String word) {
        this.word = word;
    }
    
    public int getFrequency() {
        return frequency;
    }
    
    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    public static KafkaEvent fromString(String eventStr) {
        log.info("kafka event: {}", eventStr);
        String[] split = eventStr.split(",");
        if (split.length < 3) {
            return empty();
        }
        return new KafkaEvent(split[0], Integer.valueOf(split[1]), Long.valueOf(split[2]));
    }
    
    @Override
    public String toString() {
        return word + "," + frequency + "," + timestamp;
    }
}
