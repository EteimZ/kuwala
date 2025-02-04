import React, {useState, useEffect} from "react";
import Header from "../components/Header";
import { useNavigate } from "react-router-dom";
import {useStoreActions, useStoreState} from "easy-peasy";

export default () => {
    // TODO: Move this to state store
    const { availableDataSource } = useStoreState((state) => state.canvas)
    const {setAvailableDataSource, getAvailableDataSource, setSelectedSources, saveSelectedSources} = useStoreActions((actions) => actions.canvas)
    const [isItemSelected, setIsItemSelected] = useState(false)
    const navigate = useNavigate();

    useEffect(()=>{
        getAvailableDataSource()
    }, [])

    const updateSelectionIndicator = (index) => {
        const newDS = [...availableDataSource]
        newDS[index].isSelected = !newDS[index].isSelected;
        setAvailableDataSource(newDS)
        setIsItemSelected(scanSelected())
    }

    const saveSelectedSource = () => {
        // TODO: Set the selected & create state store
        const newSelectedAvailableSource = availableDataSource.filter((e) => e.isSelected)
        setSelectedSources(newSelectedAvailableSource)
        saveSelectedSources()
    }

    const scanSelected = () => {
        let selected = false;
        for (let elements of availableDataSource) {
            if (elements.isSelected) {
                selected = true;
                break;
            }
        }
        return selected
    }



    const renderDataSources = () => {
        if(availableDataSource.length <= 0){
            return (
                <div>
                    No data sources available
                </div>
            )
        }

        // TODO: Figure out how to create grid elements
        return availableDataSource.map((e,i) => {
            return (
                <div>
                    <div
                        className={`
                            flex flex-col justify-center items-center bg-white rounded-xl cursor-pointer drop-shadow-lg
                            ${e.isSelected ? 'border-kuwala-green border-4' : ''}
                        `}
                        key={i}
                        style={{width: 148, height:148}}
                        onClick={() => {
                            // Set as an Active
                            updateSelectionIndicator(i)
                        }}
                    >
                        <img
                            src={e.logo}
                            style={{height: 72, width: 72}}
                        />
                        <span className={'mt-1'}>{e.name}</span>
                    </div>
                </div>
            )
        })
    }

    return (
        <div className={`flex flex-col h-screen overflow-y-hidden antialiased text-gray-900`}>
            <Header />
            <main className={'flex flex-col h-full w-full bg-kuwala-bg-gray py-12 px-20'}>
                <span className={'font-semibold text-3xl'}>
                    Data Catalog
                </span>
                <span className={'font-light text-xl mt-3'}>
                    Select the data source you want to connect
                </span>

                {/* Data Sources Container*/}
                <div className={'mt-10 h-5/6 space-x-8 flex flex-row'}>
                    {renderDataSources()}
                </div>

                <div className={'flex flex-row-reverse'}>
                    <button
                        className={`
                            text-white rounded-md px-4 py-2 mt-4
                            ${isItemSelected ? 'bg-kuwala-green cursor-pointer hover:text-stone-300' : 'bg-stone-300 cursor-default'}
                            `}
                        disabled={!isItemSelected}
                        onClick={()=>{
                            saveSelectedSource()
                            getAvailableDataSource() // Will reset the settings
                            navigate('/data-pipeline-management')
                        }}
                    >
                        Next
                    </button>
                </div>
            </main>
        </div>
    )
}