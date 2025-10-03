
$collectionPoints = $this->getAllRecords('collection_points')->get();
        $suppliers = $this->getAllRecords('suppliers')->get();
        $plants = $this->getAllRecords('plants')->get();
        $areaOffices = $this->getAllRecords('area_offices')->get();
        $supplierTypes = $this->getAllRecords('supplier_types')->get();
        $categories = $this->getAllRecords('categories')->get();

  $getPrices = $this->getAllRecords('prices')->where('status', 1)->orderBy('wef', 'desc')->get();
        $getArchievedBasePrices = $this->getAllRecords('archieved_prices')->where('status', 1)->orderBy('wef', 'desc')->get();

foreach ($preparedRecordsInCursor as $purchase) {

                $getBasePrice =  $this->getBasePrice($purchase, $collectionPoints, $collectionPoints, $areaOffices, $getPrices, $getArchievedBasePrices, $suppliers);

}

public function getBasePrice($purchase,$cpCollectionPoints,$mccCollectionPoints, $areaOffices, $getBasePrices,$getArchievedBasePrices, $suppliers)
    {
        if (isset($purchase['price'])) {
            return $purchase['price'];
        }
        $cpRecord='';
        $aoRecord= '';

        if (isset($purchase['cp_id'])) {
            $getCP = $cpCollectionPoints->firstWhere('_id', $purchase['cp_id']);
            $cpRecord = $getCP;
           if(isset($getCP['area_office_id'])){
               $aoRecord = $areaOffices->firstWhere('_id', $getCP['area_office_id']);
           }
           elseif(isset($getCP['plant_id'])){
            return $this->getPlantBasePrice($purchase, $getBasePrices,$getArchievedBasePrices, $suppliers);

           }
            

        } else if (isset($purchase['mcc_id'])) {
            $getCP = $mccCollectionPoints->firstWhere('_id', $purchase['mcc_id']);
            $cpRecord = $getCP;
          
            $aoRecord = $areaOffices->firstWhere('_id', $getCP['area_office_id']);

        }

        $cp = $cpRecord;

        if (isset($cp['plant_id']) || isset($purchase['plant_id'])) {
            return $this->getPlantBasePrice($purchase, $getBasePrices,$getArchievedBasePrices, $suppliers);
        }

        $ao_id = isset($purchase['area_office_id']) ? $purchase['area_office_id'] :(string) $aoRecord['_id'];
       
        $basePrice = '';
        $date = $purchase['booked_at'];
        $basePricing = $getBasePrices;

        $date = $basePricing
            ->where('wef', '<=', $purchase['booked_at'])
            ->where('area_office', $ao_id)
            ->pluck('wef')
            ->first();
        if ($date == null) {

            $basePricing = $getArchievedBasePrices;
            //  dd($basePricing,$purchase['booked_at'], $ao_id);
            // dd($purchase);
            $date = $basePricing
                ->where('wef', '<=', $purchase['booked_at'])
                ->where('area_office', $ao_id)
                ->pluck('wef')
                ->first();
                // dd($date);
                
        }

        if ($cp) {

            $basePrice = $basePricing
                ->where('area_office', $ao_id)
                ->where('source_type', $purchase['supplier_type_id'])
                ->where('supplier', $purchase['supplier_id'])
                ->where('collection_point', (string) $cp['_id'])
                ->where('wef', '=', $date)
                ->first();
               

        }

        if ($basePrice == null) {
            $basePrice = $basePricing
                ->where('area_office', $ao_id)
                ->where('source_type', $purchase['supplier_type_id'])
                ->where('supplier', $purchase['supplier_id'])
                ->where('collection_point', null)
                ->where('wef', '=', $date)
                ->first();
        }
      
        if ($basePrice == null && $cp) {
            $basePrice = $basePricing
                ->where('area_office', $ao_id)
                ->where('source_type', $purchase['supplier_type_id'])
                ->where('supplier', null)
                ->where('collection_point',(string) $cp['_id'])
                ->where('wef', '=', $date)
                ->first();
               

        }
       
        if ($basePrice == null) {
            $basePrice = $basePricing
                ->where('area_office', $ao_id)
                ->where('source_type', $purchase['supplier_type_id'])
                ->where('supplier', null)
                ->where('collection_point', null)
                ->where('wef', '=', $date)
                ->first();
        }
       
        if ($basePrice) {
            return $basePrice['price'];
        } else {
            return 0;
        }
    }


    public function getPlantBasePrice($purchase, $getBasePrices,$getArchievedBasePrices, $suppliers)
    {
        $getsupplier = $suppliers->firstWhere('_id', $purchase['supplier_id']);
        
        if (isset($purchase['price'])) {
            return $purchase['price'];
        }
   
        $basePrice = $getBasePrices
            ->where('source_type', $getsupplier['supplier_type_id'])
            ->where('supplier', $purchase['supplier_id'])
            ->where('wef', '<=', $purchase['booked_at'])
            ->sortByDesc('wef')
            ->first();
          
        if ($basePrice == null) {
            $basePrice = $getBasePrices
                ->where('source_type', $getsupplier['supplier_type_id'])
                ->where('supplier', null)
                ->where('wef', '<=', $purchase['booked_at'])
                ->first();
        }
        if ($basePrice) {
            return $basePrice['price'];
        }else {
            // return 0;
           $getarchbaseprice= $getArchievedBasePrices->where('source_type', $getsupplier['supplier_type_id'])
            ->where('supplier', $purchase['supplier_id'])
            ->where('wef', '<=', $purchase['booked_at'])
            ->sortByDesc('wef')
            ->first();
            if ($getarchbaseprice == null) {
                $getarchbaseprice = $getArchievedBasePrices
                    ->where('source_type', $getsupplier['supplier_type_id'])
                    ->where('supplier', null)
                    ->where('wef', '<=', $purchase['booked_at'])
                    ->first();
            }
            if ($getarchbaseprice) {
                return $getarchbaseprice['price'];
            }else{
                return 0;
            }

        
        
        }
        
    }
            
