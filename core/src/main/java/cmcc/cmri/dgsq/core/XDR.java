/*
 *
 */

package cmcc.cmri.dgsq.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


// XDR bean
public class XDR {

    private static final Logger logger = LogManager.getLogger(XDR.class);

    // xdr vendor
    private String vendor;

    // xdr device
    private String device;

    // xdr interface
    private String name;

    // xdr date, format is yyyymmddHHmmss
    private String date;

    // xdr file sequence
    private String sequence;

    // xdr file delimiter, default is "|"
    private String delimiter;

    // xdr file type, support txt, cvs
    private String type;

    XDR() {

    }

    // Pharse XDR with standard file pattern: [interface]_[yyyymmddHHmmss]_vendor_device_sequence.txt
    XDR(String file) {

        try {
            this.type = file.split("\\.")[1];

            String pattern = file.split("\\.")[0];
            String[] patterns = pattern.split("_");

            this.name = patterns[0];
            this.date = patterns[1];
            this.vendor = patterns[2];
            this.device = patterns[3];
            this.sequence = patterns[4];

            this.delimiter = AppSettings.config.getString("xdr.file.delimiter");

            logger.debug("XDR is [{}] [{}] [{}] [{}] [{}] [{}] [{}]",
                    this.name, this.date, this.vendor, this.device, this.sequence, this.type, this.delimiter);

        } catch (Exception e) {
            logger.error("Cannot phrase xdr file {}.", file);
            e.printStackTrace();
        }

    }

    public String getVendor() {
        return vendor;
    }

    public void setVendor(String vendor) {
        this.vendor = vendor;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getSequence() {
        return sequence;
    }

    public void setSequence(String sequence) {
        this.sequence = sequence;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }


    public static void main(String[] args) {

        logger.trace("Entering");
        XDR xdr = new XDR("http_20180920143741_01_001_001.txt");
        logger.trace("Exiting");
    }
}
