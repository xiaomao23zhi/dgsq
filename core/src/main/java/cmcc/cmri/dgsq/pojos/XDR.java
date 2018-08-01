/*
 *
 */

package cmcc.cmri.dgsq.pojos;

import cmcc.cmri.dgsq.run.AppSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;


// XDR bean
public class XDR implements Serializable {

    private static final Logger logger = LogManager.getLogger(XDR.class);

    // xdr vendor
    private String vendor;

    // xdr device
    private String device;

    // xdr interface
    private String name;

    // xdr schemas
    private String schemas;

    // xdr date, format is yyyy-mm-dd HH:mm:ss
    private String date;

    // xdr file sequence
    private String sequence;

    // xdr file delimiter, default is "|"
    private String delimiter;

    // xdr file type, support txt, cvs
    private String type;

    // xdr file name
    private String file;

    XDR() {

    }

    // Pharse XDR with standard file pattern: [name]_[yyyymmddHHmmss]_[vendor]_[device]_[sequence].txt
    public XDR(String xdrFile) {

        logger.trace("Phrase XDR from {}", xdrFile);

        try {
            // http_20180703150000_01_001_000.txt
            this.file = xdrFile.substring(xdrFile.lastIndexOf("/") + 1);
            this.type = file.split("\\.")[1];

            String pattern = file.split("\\.")[0];
            logger.debug("pattern is [{}]", pattern);
            String[] patterns = pattern.split("_");

            Date dateString = new SimpleDateFormat("yyyyMMddHHmmss").parse(patterns[1]);

            this.name = patterns[0];
            this.date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dateString);
            this.vendor = patterns[2];
            this.device = patterns[3];
            this.sequence = patterns[4];

            this.delimiter = AppSettings.config.getString("xdr.file.delimiter");
            this.schemas=AppSettings.config.getString("xdr.schema." + this.name);

            logger.debug("XDR is [{}] [{}] [{}] [{}] [{}] [{}] [{}] [{}] [{}]",
                    this.file, this.name, this.date, this.vendor, this.device, this.sequence, this.type, this.delimiter, this.schemas);

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

    public String getSchemas() {
        return schemas;
    }

    public void setSchemas(String schemas) {
        this.schemas = schemas;
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

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public static void main(String[] args) {

        logger.trace("Entering");
        XDR xdr = new XDR("hdfs:///user/hadoop/dgsq/xdr/in/20180703/http_20180703150000_01_001_000.txt");
        logger.trace("Exiting");

        logger.trace(AppSettings.config.getString("app.version"));
    }
}
