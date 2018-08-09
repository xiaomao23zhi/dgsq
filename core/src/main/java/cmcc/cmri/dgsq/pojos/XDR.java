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
    private String inerface;

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

    // xdr long name
    private String name;

    XDR() {

    }

    // Pharse XDR with standard file pattern: [name]_[yyyymmddHHmmss]_[vendor]_[device]_[sequence].txt
    public XDR(String xdrFile, String xdrSchema) {

        logger.trace("Phrase XDR from {}", xdrFile);

        try {
            // http_20180703150000_01_001_000.txt
            this.file = xdrFile.substring(xdrFile.lastIndexOf("/") + 1);
            this.type = file.split("\\.")[1];

            this.name = file.split("\\.")[0];
            logger.debug("pattern is [{}]", name);
            String[] patterns = name.split("_");

            Date dateString = new SimpleDateFormat("yyyyMMddHHmmss").parse(patterns[1]);

            this.inerface = patterns[0];
            this.date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dateString);
            this.vendor = patterns[2];
            this.device = patterns[3];
            this.sequence = patterns[4];

            this.delimiter = AppSettings.config.getString("xdr.file.delimiter");
            this.schemas = xdrSchema;

            logger.debug("XDR is [{}] [{}] [{}] [{}] [{}] [{}] [{}] [{}] [{}]",
                    this.file, this.inerface, this.date, this.vendor, this.device, this.sequence, this.type, this.delimiter, this.schemas);

        } catch (Exception e) {
            logger.error("Cannot phrase xdr file {}.", file);
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {

        logger.trace("Entering");
        XDR xdr = new XDR("hdfs:///user/hadoop/dgsq/xdr/in/20180703/http_20180703150000_01_001_000.txt", "Length,Local_Province,Local_City,Owner_Province,Owner_City,Roaming_Type,Interface,xDR_ID,RAT,IMSI,IMEI_SV,MSISDN,Machine_IP_Add_type,SGW_GGSN_IP_Add,eNB_SGSN_IP_Add,PGW_Add,SGW_GGSN_Port,eNB_SGSN_Port,PGW_Port,eNB_SGSN_GTP_TEID,SGW_GGSN_GTP_TEID,TAC,Cell_ID,APN,App_Type_Code,Procedure_Start_Time,Procedure_End_Time,longitude,latitude,Height,Coordinate_system,Protocol_Type,App_Type,App_Sub_type,App_Content,App_Status,IP_address_type,USER_IPv4,USER_IPv6,User_Port,L4_protocal,App_Server_IP_IPv4,App_Server_IP_IPv6,App_Server_Port,UL_Data,DL_Data,UL_IP_Packet,DL_IP_Packet,updura,downdura,UL_Disorder_IP_Packet,DL_Disorder_IP_Packet,UL_Retrans_IP_Packet,DL_Retrans_IP_Packet,TCP_Response_Time,TCP_ACK_Time,UL_IP_FRAG_PACKETS,DL_IP_FRAG_PACKETS,First_Req_Time,First_Response_Time,Window,MSS_,TCP_SYN_Num,TCP_Status,Session_End,TCP_SYN_ACK_Mum,TCP_ACK_Num,TCP1_2_Handshake_Status,TCP2_3_Handshake_Status,UL_ProbeID,UL_LINK_Index,DL_ProbeID,DL_LINK_Index,TransactionID,Flow_Control,UL_AVG_RTT,DW_AVG_RTT,User_Account,Refer_XDR_ID,Rule_source,unkown1,unkown2,unkown3,unkown4,unkown5,unkown6,unkown7,unkown8,unkown9,unkown10,unkown11,unkown12,unkown13,unkown14,unkown15");
        logger.trace("Exiting");

        logger.trace(AppSettings.config.getString("app.version"));
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

    public String getInerface() {
        return inerface;
    }

    public void setInerface(String name) {
        this.inerface = name;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getSchemas() {
        return schemas;
    }

    public void setSchemas(String schemas) {
        this.schemas = schemas;
    }

    public String getSequence() {
        return sequence;
    }

    public void setSequence(String sequence) {
        this.sequence = sequence;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
}
