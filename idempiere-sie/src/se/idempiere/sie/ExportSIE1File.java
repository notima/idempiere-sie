package se.idempiere.sie;

import java.sql.Timestamp;
import java.util.List;
import java.util.logging.Level;

import org.compiere.Adempiere;
import org.compiere.model.MClient;
import org.compiere.model.MOrg;
import org.compiere.model.MOrgInfo;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.process.SvrProcess;
import org.notima.sie.RARRec;
import org.notima.sie.SIEFile;
import org.notima.sie.SIEFileType1;


public class ExportSIE1File extends SvrProcess {

	private int		p_acctSchemaId;
	private int		p_orgId;
	private	String	p_fileName;
	
	/** Invoice Date From		*/
	private Timestamp	p_Date_From = null;
	/** Invoice Date To			*/
	private Timestamp	p_Date_To = null;
	
	@Override
	protected void prepare() {
		
        // Get parameters
        ProcessInfoParameter[] para = getParameter();
        for (int i = 0; i < para.length; i++) {
            String name = para[i].getParameterName();
            if (para[i].getParameter() == null);
              else if (name.equals("C_AcctSchema_ID")) {
				p_acctSchemaId = para[i].getParameterAsInt();
            } else if (name.equals("AD_Org_ID")) {
            	p_orgId = para[i].getParameterAsInt();
            } else if (name.equals("FileName")) {
            	p_fileName = para[i].getParameter().toString();
            } else if (name.equals("DateAcct"))
			{
				p_Date_From = (Timestamp)para[i].getParameter();
				p_Date_To = (Timestamp)para[i].getParameter_To();
			}
            else {
                log.log(Level.SEVERE, "Unknown Parameter: " + name);
            }
        }
		
	}
	
	@Override
	protected String doIt() throws Exception {

		// Make sure filename ends with si
		if (!p_fileName.toLowerCase().endsWith(".si")) {
			p_fileName = p_fileName + ".si";
		}
		
		// Create new SIE1 file
		SIEFileType1 sieFile = new SIEFileType1(p_fileName);
		sieFile.setProgram(Adempiere.getSum());
		sieFile.setKpTyp("EUBAS97");
		MClient client = new MClient(getCtx(), this.getAD_Client_ID(), get_TrxName());
		if (p_orgId>0) {
			MOrg org = new MOrg(getCtx(), p_orgId, get_TrxName());
			sieFile.setFNamn(org.getName());
			MOrgInfo info = org.getInfo();
			sieFile.setOrgNr(info.getTaxID());
		} else {
			sieFile.setFNamn(client.getName());
		}
		
		List<RARRec> rars = AdempiereSIEUtil.getFiscalYears(getCtx(), this.getAD_Client_ID(), p_orgId, p_Date_To, 1, get_TrxName());

		for (RARRec rar : rars) {
			sieFile.addRARRec(rar);
			AdempiereSIEUtil.createAccountList(rar, p_Date_From, p_Date_To, p_acctSchemaId, p_orgId, (SIEFile)sieFile, get_TrxName());			
		}
		
		System.out.println("createAccountList done");
		
		sieFile.writeToFile();
		
		return("");
	}
	
	
}
