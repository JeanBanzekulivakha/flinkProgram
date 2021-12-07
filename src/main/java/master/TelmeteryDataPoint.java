package master;

public class TelmeteryDataPoint {

    public Integer vehicleId;
    public Integer timestamp;
    public Integer speed;
    public Integer highway;
    public Integer lane;
    public Integer direction; //Could be an Enum
    public Integer segment;
    public long position;

    public TelmeteryDataPoint(){}

    public static TelmeteryDataPoint fromString(String line) {

        String[] tokens = line.split("(,|;)\\s*");

        if (tokens.length != 8) {
            throw new RuntimeException("Invalid record: " + line);
        }

        TelmeteryDataPoint event = new TelmeteryDataPoint();

        try {
            event.timestamp = Integer.parseInt(tokens[0]);//DateTime.parse(tokens[0], timeFormatter).getMillis();
            event.vehicleId = Integer.parseInt(tokens[1]);
            event.speed = Integer.parseInt(tokens[2]);
            event.highway = Integer.parseInt(tokens[3]);
            event.lane = Integer.parseInt(tokens[4]);
            event.direction = Integer.parseInt(tokens[5]);
            event.segment = Integer.parseInt(tokens[6]);
            event.position = Long.parseLong(tokens[7]);

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid field: " + line, nfe);
        }

        return event;
    }

    public String toString() {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s",
                timestamp, vehicleId, speed, highway, lane, direction, segment, position);
    }
}
