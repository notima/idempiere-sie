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
import org.notima.sie.SIEFileType4;


/**
 * Creates a SIE4 file.
 * 
 * 
 * @author Daniel Tamm
 *
 */
public class ExportSIE4File extends SvrProcess {

	private int		p_acctSchemaId;
	private int		p_orgId;
	private	String	p_fileName;
	protected boolean	p_onlyTransactions;
	protected int		p_tableId = 0;
	
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
			} else if (name.equals("TransactionsOnly")) {
				p_onlyTransactions = para[i].getParameterAsBoolean();
			} else if (name.equals("AD_Table_ID")) {
				p_tableId = para[i].getParameterAsInt();
			}
            
            else {
                log.log(Level.SEVERE, "Unknown Parameter: " + name);
            }
        }
		
	}
	
	@Override
	protected String doIt() throws Exception {

		// Create new SIE4 file
		SIEFileType4 sieFile = new SIEFileType4(p_fileName);
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
		// Get calendars to determine fiscal years
		List<RARRec> rars = AdempiereSIEUtil.getFiscalYears(getCtx(), this.getAD_Client_ID(), p_orgId, p_Date_To, 0, get_TrxName());

		for (RARRec rar : rars) {
			sieFile.addRARRec(rar);
			AdempiereSIEUtil.createAccountList(rar, p_Date_From, p_Date_To, p_acctSchemaId, p_orgId, sieFile, get_TrxName());
		}
		
		System.out.println("createAccountList done");
		AdempiereSIEUtil.createVerRecs(p_Date_From, p_Date_To, p_acctSchemaId, p_orgId, sieFile, get_TrxName());
		
		sieFile.writeToFile();
		
		return("");
	}

	
	
	
	/**
	 * Returns voucher series.
	 */
//	private 

}
